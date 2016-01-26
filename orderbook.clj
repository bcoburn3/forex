(ns stockfighter-server.ob
  (require [clj-http.client :as client]
           [gniazdo.core :as ws]
           [clojure.data.json :as json]
           [clojure.math.combinatorics :as combo]
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
  (assoc book [(order "price") (order "id")] order))

(defn match-order [book qty max-price dir-comp]
  ;returns a vector [remaining-qty fills filled-orders order-book]"
  ;returned order book doesn't have order to be matched, has all other
  ;effects of that order
  (loop [fills [] filled []
         cur-order (first book) cur-book (next book) rem-qty qty]
    (let [cur-qty (cur-order "qty")
          cur-price (cur-order "price")
          ts (System/currentTimeMillis)] ;TODO:  make this a RFC 3339 timestamp
      (if (dir-comp max-price cur-price)
        (let [match-qty (min rem-qty cur-qty)
              match-order (assoc cur-order
                                 "qty" (- cur-qty match-qty)
                                 "totalFilled" (+ (cur-order "totalFilled") match-qty)
                                 "fills" (conj (cur-order "fills")
                                               {"qty" match-qty "price" cur-price "ts" ts}))]
          (cond (> rem-qty cur-qty)
                (recur (conj fills {"qty" cur-qty "price" cur-price "ts" ts})
                       (conj filled (assoc match-order "open" false))
                       (first cur-book) (next cur-book) (- rem-qty cur-qty))
                (= rem-qty cur-qty)
                [0
                 (conj fills {"qty" cur-qty "price" cur-price "ts" ts})
                 (conj filled (assoc match-order "open" false))
                 cur-book]
                (< rem-qty cur-qty)
                [0
                 (conj fills {"qty" rem-qty "price" cur-price "ts" ts})
                 filled
                 (assoc cur-book [cur-price (cur-order "id")] match-order)]))
        [rem-qty fills filled (add-order cur-order cur-book) cur-order]))))

(defn make-orderbook [in-ch]
  ;returns a channel on which order results will appear
  (let [out-ch (chan 100)]
    (go-loop [bids (sorted-map-by (comp-order >))
              asks (sorted-map-by (comp-order <))
              msg (<! in-ch)]
      (if (msg "cancel")
        (let [{buy? "buy?" key "key"} msg
              book (if buy? bids asks)
              order (book key)]
          (do (>! out-ch (assoc order "open" false "qty" 0))
              (recur (if buy? (dissoc book key) bids) (if buy? asks (dissoc book key))
                     (<! in-ch))))
        (let [{dir "direction" order-type "orderType" price "price" qty "qty"} msg
              buy? (= dir "buy")
              dir-comp (if buy? buy-comp sell-comp) ;function to use to see if orders match
              book (if buy? asks bids)]
          (let [[rem-qty fills filled-orders new-book] (match-order book msg qty price dir-comp)
                new-order (assoc msg
                                 "qty" rem-qty
                                 "totalFilled" (- (msg "originalQty") rem-qty)
                                 "fills" fills)]
            (if (> rem-qty 0)
              (case order-type
                "limit" (do (go (>! out-ch new-order))
                            (go (>! out-ch {"ws-fills" filled-orders}))
                            (recur (if buy? bids new-book) (if buy? new-book asks)
                                   (<! in-ch)))
                "fok" (do (go (>! out-ch (assoc new-order "qty" 0 "open" false)))
                          (recur bids asks (<! in-ch))) ;order killed, recur with original values
                "ioc" (do (go (>! out-ch (assoc new-order "qty" 0 "open" false)))
                          (go (>! out-ch {"ws-fills" filled-orders}))
                          (recur (if buy? bids new-book) (if buy? new-book asks)
                                   (<! in-ch)))) 
                                        ;market orders will be treated as IOC orders with 0 or
                                        ;max int prices
              (do (go (>! out-ch (assoc new-order "open" false)))
                  (go (>! out-ch {"ws-fills" filled-orders}))
                  (recur (if buy? bids new-book) (if buy? new-book asks)
                         (<! in-ch))))))))
    out-ch))
