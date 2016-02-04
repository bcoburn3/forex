(ns forex.ob
  (require [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go go-loop chan buffer close! thread
                     alts! alts!! timeout]]))

;The basic strategy here is to store seperate, sorted, maps for bids and asks
;indexed by [price id], so that everything compares correctly.  The frontend
;is responsible to always assigning id's in strict time order, and this design
;should make all three of place, match, and cancel take ~log n time

(defn buy-comp [price best-price]
  (if best-price
    (>= price best-price)
    true))

(defn sell-comp [price best-price]
  (if best-price
    (<= price best-price)
    true))

(defn comp-order [comp-func]
  (fn [[p1 id1] [p2 id2]]
    (cond (comp-func p1 p2) ;price priority, 'better' price by comp-func is higher priority
          true
          (= p1 p2)
          (< id1 id2) ;time priority, lower id implies earlier order implies higher priority
          :else
          false))) ;p2 must have higher price priority

(defn add-order [order book]
  (assoc book [(order "price") (order "id")] order))

(defn update-match-order [order fill fill-qty open?]
  (assoc order
         :open open?
         :fills (conj (order :fills) fill)
         :totalFilled (+ (order :totalFilled) fill-qty)
         :qty (- (order :qty) fill-qty)))

(defn match-order [book qty max-price dir-comp]
  ;returns a vector [remaining-qty fills filled-orders order-book]"
  ;returned order book doesn't have order to be matched, has all other
  ;effects of that order
  (loop [fills [] changed-orders []
         cur-order (first book) cur-book (next book) rem-qty qty]
    (let [cur-qty (cur-order "qty")
          cur-price (cur-order "price")
          ts (System/currentTimeMillis)] ;TODO:  make this a RFC 3339 timestamp
      (if (dir-comp max-price cur-price)
        (let [match-qty (min rem-qty cur-qty)
              fill {:qty match-qty :price cur-price :ts ts}
              new-fills (conj fills fill)
              match-order (update-match-order cur-order fill match-qty (< rem-qty cur-qty))
              new-changed-orders (conj changed-orders match-order)]
          (cond (> rem-qty cur-qty)
                (recur new-fills new-changed-orders (first cur-book) 
                       (next cur-book) (- rem-qty cur-qty))
                (= rem-qty cur-qty)
                [0 fills new-changed-orders cur-book]
                (< rem-qty cur-qty)
                [0 fills new-changed-orders (add-order match-order cur-book)]))
        [rem-qty fills filled cur-book cur-order]))))

(defn make-orderbook [from-frontend-ch to-mg-ch to-exec-ch]
    (go-loop [bids (sorted-map-by (comp-order >))
              asks (sorted-map-by (comp-order <))
              [type req-id msg] (<! from-frontend-ch)]
      (case type
        :cancel-req
        (let [{buy? :buy? key :key} msg
              book (if buy? bids asks)
              order (book key)]
          (do (>! to-mg-ch [type req-id (assoc order :qty 0 :open false)])
              (recur (if buy? (dissoc book key) bids) (if buy? asks (dissoc book key))
                     (<! from-frontend-ch))))
        :place-req
        (let [{dir :direction order-type :orderType price :price qty :qty} msg
              buy? (= dir :buy)
              dir-comp (if buy? buy-comp sell-comp) ;function to use to see if orders match
              book (if buy? asks bids)]
          (let [[rem-qty fills changed-orders new-book] (match-order book msg qty price dir-comp)
                new-order (assoc msg
                                 :qty rem-qty
                                 :totalFilled (- (msg :originalQty) rem-qty)
                                 :fills fills)]
            (if (> rem-qty 0)
              (case order-type
                "limit" 
                (do (go (>! to-mg-ch [type req-id new-order]))
                    (go (>! to-exec-ch changed-orders))
                    (recur (if buy? (add-order new-order bids) new-book) 
                           (if buy? new-book (add-order new-order asks))
                           (<! from-frontend-ch)))
                ("fok" "fill-or-kill") 
                (do (go (>! to-mg-ch [type req-id (assoc new-order "qty" 0)]))
                    (recur bids asks (<! from-frontend-ch))) ;order killed, recur with original values
                ("ioc" "immediate-or-cancel" "market")
                (do (go (>! to-mg-ch [type req-id (assoc new-order "qty" 0)]))
                    (go (>! to-exec-ch changed-orders))
                    (recur (if buy? bids new-book) (if buy? new-book asks)
                           (<! from-frontend-ch)))) 
              (do (go (>! to-mg-ch [type req-id (assoc new-order "open" false)]))
                  (go (>! to-exec-ch changed-orders))
                  (recur (if buy? bids new-book) (if buy? new-book asks)
                         (<! from-frontend-ch)))))))))
