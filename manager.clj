(ns forex.mg
  (require [forex.ob :as ob]
           [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go go-loop chan buffer close! thread
                     alts! alts!! timeout]])
  (:gen-class))

;frontend responsibilities, for place:
;id, timestamp, originalQty, ok, initial open=true

;manager responsibilities, for place/cancel:
;maintain all-orders, id to account map, account to ids map, quotes, dispatch to quotes ws
;for cancel only:  buy?, key

;orderbook responsibilities, for place/cancel:
;qty, fills, open, totalFilled, maintain orderbook, dispatch to exec ws

(defn update-all-orders [all-orders changed-orders]
  (reduce-kv (fn [res id order]
               (assoc res id order)
               all-orders
               changed-orders)))

(defn new-half-quote [book]
  (if (first book)
    (let [price (first (first book))] ;orders are indexed by [price id]
      (loop [size 0 depth 0 [[cur-price id] cur-size] (first book) next-book (next book)]
        (if next-book
          (recur (if (= price cur-price) (+ size cur-size) size) 
                 (+ depth cur-size) (first next-book) (next next-book))
          [price (if (= price cur-price) (+ size cur-size) size) (+ depth cur-size)])))
    [0 0 0]))

(defn update-bid-ask [quote bids asks]
  (let [[bid bidsize biddepth] (new-half-quote book)
        [ask asksize askdepth] (new-half-quote book)]
    (assoc quote 
           :bid bid :bidSize bidsize :bidDepth biddepth
           :ask ask :askSize asksize :askDepth askdepth)))

(defn update-quote-trade [quote fills to-quote-ws-ch]
  (if (first fills)
    (last (map (fn [{price :price qty :qty ts :ts}]
                 (let [res (assoc quote :last price :lastSize qty :lastTrade ts)]
                   (go (>! to-quote-ws-ch res))
                   res)))
          fills)
    quote))       

(defn make-manager [from-frontend-ch from-ob-ch to-frontend-ch to-ob-ch]
  ;incoming messages to be [type req-id actual-message]
  ;responses to be [req-id message]
  (let [chans [from-frontend-ch from-ob-ch]]
    (go-loop [[[type req-id msg] ch] (alts! chans) all-orders {} accounts-ids {} ids-account {}]
      (case type
        :place-resp
        (let [[order changed-orders fills new-bids new-asks] msg
              {account :account id :id} order
              new-all-orders (update-all-orders all-orders changed-orders)
              new-accounts-ids (update accounts-ids account conj id)
              new-ids-account (assoc ids-account id account)
              new-bid-ask (update-bid-ask quote new-bids new-asks)
              new-quote (update-quote-trade new-bid-ask fills)]
          (when (not (= new-quote quote))
            (go (>! to-quote-ws-ch new-quote)))
          (>! to-frontend-ch [req-id (place-update-fields order)])
          (recur (alts! chans) new-all-orders new-accounts-ids new-ids-accounts
                 new-quote new-bids new-asks))
        :cancel-req
        (let [order (all-orders msg)
              key [(order :price) msg]
              buy? (= (order :direction) "buy")]
          (do (go (>! to-ob-ch [:cancel-req req-id {:buy? buy? :key key}]))
              (recur (alts! chans) all-orders accounts-ids ids-accounts
                     quote bids asks)))
        :cancel-resp
        (let [[order new-bids new-asks] msg
              new-all-orders (assoc all-orders id order)
              new-quote (update-bid-ask quote new-bids new-asks)]
          (when (not (= new-quote quote))
            (go (>! to-quote-ws new-quote)))
          (>! to-frontend-ch [req-id order])
          (recur (alts! chans) new-all-orders accounts-ids ids-accounts
                 new-quote new-bids new-asks))
        :quote-req
        (do (go (>! to-frontend-ch [req-id quote]))
            (recur (alts! chans) all-orders accounts-ids ids-accounts
                   quote bids asks))
        :orderbook-req
        (do (go (let [ob (gen-ob bids asks)]
                  (>! to-frontend-ch [req-id ob])))
            (recur (alts! chans) all-orders accounts-ids ids-accounts
                   quote bids asks))
        :one-order-status-req
        (do (go (>! to-frontend-ch [req-id (all-orders (msg :id))]))
            (recur (alts! chans) all-orders accounts-ids ids-accounts
                   quote bids asks))
        :all-order-status-req
        (do (go (let [resp (gen-all-status all-orders (accounts-ids (msg :account)))]
                  (>! to-frontend-ch [req-id resp])))
            (recur (alts! chans) all-orders accounts-ids ids-accounts
                   quote bids asks))))))
