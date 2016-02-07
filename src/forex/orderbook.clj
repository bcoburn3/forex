(ns forex.orderbook
  (:require [forex.timestamp :as ts :refer [get-timestamp]]
            [clojure.core.async
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
  (assoc book [(order :price) (order :id)] order))

(defn update-match-order [order fill fill-qty open?]
  (let [res (assoc order
                   :open open?
                   :fills (conj (order :fills) fill)
                   :totalfilled (+ (order :totalfilled) fill-qty)
                   :qty (- (order :qty) fill-qty))]
    res))

(defn match-order [book qty max-price dir-comp market?]
  ;returns a vector [remaining-qty fills filled-orders order-book]"
  ;returned order book doesn't have order to be matched, has all other
  ;results of that order
  (let [first-entry (first book)]
    (if first-entry
      (loop [fills [] changed-orders [] cur-entry first-entry 
             cur-book (dissoc book (first first-entry)) rem-qty qty]
        (let [[[cur-price cur-id] cur-order] cur-entry
              cur-qty (cur-order :qty)
              ts (get-timestamp)] 
          (if (or (dir-comp max-price cur-price) market?)
            (let [match-qty (min rem-qty cur-qty)
                  fill {:qty match-qty :price cur-price :ts ts}
                  new-fills (conj fills fill)
                  match-order (update-match-order cur-order fill match-qty (< rem-qty cur-qty))
                  new-changed-orders (conj changed-orders match-order)]
              (cond (> rem-qty cur-qty)
                    (let [next-entry (first cur-book)]
                      (if next-entry
                        (recur new-fills new-changed-orders next-entry
                               (dissoc cur-book (first next-entry)) (- rem-qty cur-qty))
                        [rem-qty fills new-changed-orders cur-book]))
                    (= rem-qty cur-qty)
                    [0 new-fills new-changed-orders cur-book]
                    (< rem-qty cur-qty)
                    [0 new-fills new-changed-orders (add-order match-order cur-book)]))
            [rem-qty fills changed-orders cur-book])))
      [qty [] [] book])))

(defn make-orderbook [to-ob-ch ob-mg-ch to-exec-ch mg-router-ch]
    (go-loop [bids (sorted-map-by (comp-order >))
              asks (sorted-map-by (comp-order <))
              [req-type req-id msg] (<! to-ob-ch)]
      (case req-type
        :cancel-req
        (let [{buy? :buy? order-key :key} msg
              book (if buy? bids asks)
              order (book order-key)]
          (go (if order 
                (>! ob-mg-ch [:cancel-resp req-id [(assoc order :qty 0 :open false) 
                                                   (if buy? (dissoc book order-key) bids) 
                                                   (if buy? asks (dissoc book order-key))]])
                (>! ob-mg-ch [:cancel-resp req-id [(second order-key) bids asks]]))) ;order must not be open
          (recur (if buy? (dissoc book order-key) bids) (if buy? asks (dissoc book order-key))
                 (<! to-ob-ch)))
        :place-req
        (let [{dir :direction order-type :ordertype price :price qty :qty} msg
              buy? (= dir "buy")
              dir-comp (if buy? buy-comp sell-comp) ;function to use to see if orders match
              standing-book (if buy? asks bids)
              market? (= order-type "market")]
          (let [[rem-qty fills changed-orders new-book] (match-order standing-book qty price dir-comp market?)
                new-order (assoc msg
                                 :qty rem-qty
                                 :totalfilled (- (msg :originalqty) rem-qty)
                                 :fills fills)]
            (if (> rem-qty 0)
              (case order-type
                "limit" 
                (do (go (>! ob-mg-ch [:place-resp req-id [new-order changed-orders fills 
                                                          (if buy? (add-order new-order bids) new-book) 
                                                          (if buy? new-book (add-order new-order asks))]]))
                    (go (>! to-exec-ch changed-orders))
                    (recur (if buy? (add-order new-order bids) new-book) 
                           (if buy? new-book (add-order new-order asks))
                           (<! to-ob-ch)))
                ("fok" "fill-or-kill") 
                (do (go (>! ob-mg-ch [:place-resp req-id [(assoc new-order :qty 0) [] [] ;no changes or fills
                                                          bids asks]]))
                    (recur bids asks (<! to-ob-ch))) ;order killed, recur with original values
                ("ioc" "immediate-or-cancel" "market")
                (do (go (>! ob-mg-ch [:place-resp req-id [(assoc new-order :qty 0) changed-orders fills
                                                          (if buy? bids new-book)
                                                          (if buy? new-book asks)]]))
                    (go (>! to-exec-ch changed-orders))
                    (recur (if buy? bids new-book) (if buy? new-book asks)
                           (<! to-ob-ch)))) 
              (do (go (>! ob-mg-ch [:place-resp req-id [(assoc new-order :open false) changed-orders fills
                                                        (if buy? bids new-book)
                                                        (if buy? new-book asks)]]))
                  (go (>! to-exec-ch changed-orders))
                  (recur (if buy? bids new-book) (if buy? new-book asks)
                         (<! to-ob-ch)))))))))
