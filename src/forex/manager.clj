(ns forex.manager
  (:require [forex.timestamp :as ts :refer [get-timestamp]]
            [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go go-loop chan buffer close! thread
                     alts! alts!! timeout]]))

;frontend responsibilities, for place:
;id, timestamp, originalQty, ok, initial open=true

;manager responsibilities, for place/cancel:
;maintain all-orders, id to account map, account to ids map, quotes, dispatch to quotes ws
;for cancel only:  buy?, key

;orderbook responsibilities, for place/cancel:
;qty, fills, open, totalFilled, maintain orderbook, dispatch to exec ws

;(def *venue* "TESTEX") ;these should probably be arguments
;(def *symbol* "FOOBAR")

(defn update-all-orders [all-orders new-order changed-orders]
  (let [with-new-order (assoc all-orders (new-order :id) new-order)]
    (if (not (= changed-orders []))
      (reduce (fn [res order]
                (let [id (order :id)]
                  (assoc res id order)))
              with-new-order
              changed-orders)
      with-new-order)))

(defn new-half-quote [book]
  (if (first book)
    (let [price (first (first book))] ;orders are indexed by [price id]
      (loop [size 0 depth 0 [[cur-price id] {qty :qty}] (first book) next-book (next book)]
        (if next-book
          (recur (if (= price cur-price) (+ size qty) size) 
                 (+ depth qty) (first next-book) (next next-book))
          [price (if (= price cur-price) (+ size qty) size) (+ depth qty)])))
    [0 0 0]))

(defn update-bid-ask [quote bids asks]
  (let [[bid bidsize biddepth] (new-half-quote bids)
        [ask asksize askdepth] (new-half-quote asks)]
    (assoc quote 
           :bid bid :bidSize bidsize :bidDepth biddepth
           :ask ask :askSize asksize :askDepth askdepth)))

(defn update-quote-trade [quote fills to-quote-ws-ch]
  (if (first fills)
    (last (map (fn [{price :price qty :qty ts :ts}]
                 (let [res (assoc quote :last price :lastsize qty :lasttrade ts)]
                   (go (>! to-quote-ws-ch res))
                   res))
          fills))
    quote))

(defn gen-ob [bids asks venue symbol]
  (let [resp-bids (if (first bids)
               (mapv (fn [[key order]] {:price (order :price) :qty (order :qty) :isbuy true})
                     bids)
               nil)
        resp-asks (if (first asks)
               (mapv (fn [[key order]] {:price (order :price) :qty (order :qty) :isbuy false})
                     asks)
               nil)]
    {:bids resp-bids :asks resp-asks :ts (get-timestamp)
     :venue venue :symbol symbol}))
    

(defn gen-all-status [all-orders ids venue]
  (let [orders (mapv all-orders ids)] ;maps are functions of their keys
    {:orders orders :venue venue})) ;it's possible this doesn't need to be a separate function

(defn make-manager [router-mg-ch ob-mg-ch mg-router-ch to-ob-ch to-quote-ws-ch venue symbol]
  ;incoming messages to be [req-type req-id actual-message]
  ;responses to be [req-id message]
  (let [chans [router-mg-ch ob-mg-ch]] 
    (go-loop [[[req-type req-id msg]] (alts! chans) all-orders {} accounts-ids {} ids-accounts {}
              quote {:bidsize 0 :biddepth 0 :asksize 0 :askdepth 0 :venue venue :symbol symbol} 
              bids (sorted-map) asks (sorted-map)]
      (case req-type 
        :place-resp
        (let [[order changed-orders fills new-bids new-asks] msg
              {account :account id :id} order
              new-all-orders (update-all-orders all-orders order changed-orders)
              new-accounts-ids (update accounts-ids account conj id)
              new-ids-accounts (assoc ids-accounts id account)
              new-bid-ask (update-bid-ask quote new-bids new-asks)
              new-quote (update-quote-trade new-bid-ask fills to-quote-ws-ch)]
          (when (not (= new-quote quote))
            (go (>! to-quote-ws-ch new-quote)))
          (>! mg-router-ch [req-id order]) ;intentionally not in a go block
          (recur (alts! chans) new-all-orders new-accounts-ids new-ids-accounts
                 new-quote new-bids new-asks))
        :cancel-req
        (let [order (all-orders msg) ;msg must be an order id as a number
              order-key [(order :price) msg]
              buy? (= (order :direction) "buy")]
          (go (>! to-ob-ch [:cancel-req req-id {:buy? buy? :key order-key}]))
          (recur (alts! chans) all-orders accounts-ids ids-accounts
                 quote bids asks))
        :cancel-resp
        (let [[order new-bids new-asks] msg]
          (if (map? order)
            (let [new-all-orders (assoc all-orders (order :id) order)
                  new-quote (update-bid-ask quote new-bids new-asks)]
              (when (not (= new-quote quote))
                (go (>! to-quote-ws-ch new-quote)))
              (>! mg-router-ch [req-id order]) ;intentionally not in a go block
              (recur (alts! chans) new-all-orders accounts-ids ids-accounts
                 new-quote new-bids new-asks))
            (do (>! mg-router-ch [req-id (all-orders order)]) ;order is set to id to be canceled if already closed
                (recur (alts! chans) all-orders accounts-ids ids-accounts
                       quote new-bids new-asks))))
        :quote-req
        (do (go (>! mg-router-ch [req-id quote]))
            (recur (alts! chans) all-orders accounts-ids ids-accounts
                   quote bids asks))
        :orderbook-req
        (do (go (>! mg-router-ch [req-id (gen-ob bids asks venue symbol)]))
            (recur (alts! chans) all-orders accounts-ids ids-accounts
                   quote bids asks))
        :one-order-status-req
        (do (go (>! mg-router-ch [req-id (all-orders msg)]))
            (recur (alts! chans) all-orders accounts-ids ids-accounts
                   quote bids asks))
        :all-order-status-req
        (do (go (let [resp (gen-all-status all-orders (accounts-ids msg))]
                  (>! mg-router-ch [req-id resp])))
            (recur (alts! chans) all-orders accounts-ids ids-accounts
                   quote bids asks))))))
