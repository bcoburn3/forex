(ns forex.handler
  (:require [forex.manager :as mg]
            [forex.orderbook :as ob]
            [forex.timestamp :as ts :refer [get-timestamp]]
            [forex.config :as cf]
            [clojure.data.json :as json]
            [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go go-loop chan buffer close! thread
                     alts! alts!! timeout]]
            [compojure.core :as cc]
            [compojure.handler :as handler]
            [compojure.route :as route])
  (:use org.httpkit.server)
  (:gen-class))

(def to-venue-manager-ch (chan 100))

(defn send-key-fn [k]
  (let [m {:isbuy "isBuy"
           :originalqty "originalQty"
           :ordertype "orderType"
           :totalfilled "totalFilled"
           :bidsize "bidSize"
           :biddepth "bidDepth"
           :asksize "askSize"
           :askdepth "askDepth"
           :lastsize "lastSize"
           :lasttrade "lastTrade"
           :quotetime "quoteTime"
           :standingid "standingId"
           :incomingid "incomingId"
           :filledat "filledAt"
           :standingcomplete "standingComplete"
           :incomingcomplete "incomingComplete"}
        res (m k)]
    (if res
      res
      (name k))))

(defn send-to-client [ch msg]
  (clojure.pprint/pprint msg)
  (send! ch {:status 200
               :headers {"Content-Type" "application/json; charset=utf-8"}
               :body (json/write-str (assoc msg :ok true)
                                     :key-fn send-key-fn)}))
(defn send-error [ch error-code error-text]
  (send! ch {:status error-code
             :headers {"Content-Type" "application/json; charset=utf-8"}
             :body (json/write-str {:ok false
                                    :error error-text})})
  nil)

(defmacro def-req-handler [handler-name req-type body-expr]
  (let [body-symb (gensym 'body-symb-)
        key-symb (gensym 'key-symb-)]
    `(defn ~handler-name [~'req]
       (with-channel ~'req ~'channel
         (go (let [~'venue (-> ~'req :params :venue) ;can't use gensyms here because place-order needs to look 
                   ~'symb (-> ~'req :params :stock)  ;at both symb and venue
                   ~key-symb (or ((~'req :headers) "x-starfighter-authorization")   ;-> macro doesn't work here
                                 ((~'req :headers) "x-stockfighter-authorization")) ;don't know why
                   ~body-symb ~body-expr])))))) ;body must be last for silly hack in place-order to work

(def-req-handler place-order :place-req
  (let [body (try (json/read-str (slurp (-> req :body))
                                       :key-fn (fn [x] (keyword (clojure.string/lower-case x))))
                  (catch Exception e (send-error channel 400 "JSON parse error")))]
     (assoc body
            :venue venue
            :symbol symb
            :price (if (= (body :ordertype) "market") 0 (body :price)))))

(def-req-handler cancel-order :cancel-req (Integer/parseInt (-> req :params :id)))

(def-req-handler orderbook :orderbook-req "orderbook")

(def-req-handler quote-req :quote-rder "quote")

(def-req-handler order-status :one-order-status-req (Integer/parseInt (-> req :params :id)))

(def-req-handler all-order-status :all-order-status-req (-> req :params :account))

(defn ws-quotes-connect [req]
  (let [venue (-> req :params :venue)
        symb (-> req :params :stock)
        account (-> req :params :account)]
    (with-channel req channel
      (go (>! to-venue-manager-ch [:ws-add-acc account channel venue symb])))))

(cc/defroutes app-routes
  (cc/GET "/ob/api/heartbeat"
          []
          {:status  200
           :headers {"Content-Type" "application/json; charset=utf-8"}
           :body (json/write-str {"ok" true "error" ""})})
  (cc/POST "/ob/api/venues/:venue/stocks/:stock/orders"
           [venue stock]
           place-order)
  (cc/POST "/ob/api/venues/:venue/stocks/:stock/orders/:id/cancel"
           [venue stock id]
           cancel-order)
  (cc/DELETE "/ob/api/venues/:venue/stocks/:stock/orders/:id"
             [venue stock id]
             cancel-order)
  (cc/GET "/ob/api/venues/:venue/stocks/:stock"
          [venue stock]
          orderbook)
  (cc/GET "/ob/api/venues/:venue/stocks/:stock/quote"
          [venue stock]
          quote-req)
  (cc/GET "/ob/api/venues/:venue/stocks/:stock/orders/:id"
          [venue stock id]
          order-status)
  (cc/GET "/ob/api/venues/:venue/accounts/:account/orders"
          [venue account]
          all-order-status)
  (cc/GET "/ob/api/venues/:venue/accounts/:account/stocks/:stock/orders"
          [venue account stock]
          all-order-status)
  (cc/GET "/ob/api/ws/:account/venues/:venue/tickertape"
          [account venue]
          ws-quotes-connect)


  (route/not-found "Not Found"))

(defn place-add-fields [req next-id]
  (assoc req
         :id next-id
         :open true
         :ts (get-timestamp)
         :originalqty (req :qty)))

(defn validate-order [order r-key ch]
  (if (not (and (order :direction) (order :qty) (order :account) (order :ordertype) (order :price)))
    (send-error ch 400 "Missing field, one of direction, qty, account, orderType, price") ;send-error returns nil
    (let [qty (order :qty)
          price (order :price)]
      (cond (not (contains cf/accounts (order :account)))
            (send-error ch 200 "that account does not exist on this venue")
            (not (= r-key (cf/accounts (order :account))))
            (send-error ch 200 "you are not authorized to post orders for that account")
            (not (number? qty))
            (send-error ch 200 "qty must be a number")
            (< qty 1)
            (send-error ch 200 "qty must be positive")
            (> qty (Integer/MAX_VALUE))
            (send-error ch 200 (str "qty must be less than " (Integer/MAX_VALUE)))
            (not (number? price))
            (send-error ch 200 "price must be a number")
            (< (order :price) 0)
            (send-error ch 200 "price must be non-negative")
            (not (contains? #{"ioc" "immediate-or-cancel" "fok" "fill-or-kill" "limit" "market"} (order :ordertype)))
            (send-error ch 200 (str (order :ordertype) " is not a valid type of order"))
            (not (contains? #{"buy" "sell"} (order :direction)))
            (send-error ch 200 (str (order :direction) " must be one of buy or sell"))
            true true))))

(defn make-router [in-ch mg-router-ch router-mg-ch to-ob-ch to-quote-ws-ch error-ch venue symb]
  ;responsible for maintaining the mapping between response channels and request ids, so
  ;each message goes back to the correct client
  (let [chans [in-ch mg-router-ch error-ch]]
    (go-loop [[msg ch] (alts! chans) next-id 0 id-chan {} next-order-id 0]
      (cond
        (= ch in-ch) ;new api request
        (let [[req-type req ch & [r-venue r-symb r-key]] msg
              new-id-chan (assoc id-chan next-id ch)
              new-next-id (+ next-id 1)]
          (if (not (= r-venue venue))
            (do (send-error ch 404 (str "No venue exists with the symbol " r-venue))
                (recur (alts! chans) next-id id-chan next-order-id))
            (case req-type
              :place-req
              (do (if (not (= r-symb symb)) 
                    (send-error ch 404 (str "symbol " r-symb " does not exist on venue " venue))
                    (if (validate-order req r-key ch)
                      (go (>! to-ob-ch [req-type next-id (place-add-fields req next-order-id)]))))
                  (recur (alts! chans) new-next-id new-id-chan (+ next-order-id 1)))
              (:cancel-req :quote-req :orderbook-req :one-order-status-req)
              (do (if (not (= r-symb symb)) 
                    (send-error ch 404 (str "symbol " r-symb " does not exist on venue " venue))
                    (go (>! router-mg-ch [req-type next-id req])))
                  (recur (alts! chans) new-next-id new-id-chan next-order-id))
              :all-order-status-req
              (do (go (>! router-mg-ch [req-type next-id req]))
                  (recur (alts! chans) new-next-id new-id-chan next-order-id))
              :ws-add-acc
              (do (go (>! to-quote-ws-ch [:add-acc [req ch]]))
                  (recur (alts! chans) new-next-id new-id-chan next-order-id)))))
        (= ch mg-router-ch) ;response to an api request
        (let [[req-id resp] msg]
          (send-to-client (id-chan req-id) resp) ;fixes up the json and sends
          (recur (alts! chans) next-id id-chan next-order-id))
        (= ch error-ch) ;an error message from the manager or orderbook
        (let [[req-id error-code error-text] msg]
          (send-error (id-chan req-id) error-code error-text)
          (recur (alts! chans) next-id id-chan next-order-id))))))

(defn make-quote-ws [to-quote-ws-ch]
  (go-loop [accounts-ws {} [req-type msg] (<! to-quote-ws-ch)]
    (case req-type
      :add-acc
      (let [[acc ch] msg]
        (recur (assoc accounts-ws acc ch) (<! to-quote-ws-ch)))
      :rem-acc
      (recur (dissoc accounts-ws msg) (<! to-quote-ws-ch))
      :send-quote
      (do (doseq [ch (vals accounts-ws)]
            (send-to-client ch msg))
          (recur accounts-ws (<! to-quote-ws-ch))))))

(defn make-venue [venue symb to-router-ch]
  (let [router-mg-ch (chan 100)
        mg-router-ch (chan 100)
        ob-mg-ch (chan 100)
        to-ob-ch (chan 100)
        error-ch (chan 100)
        to-quote-ws-ch (chan (a/sliding-buffer 100)) ;not implemented
        to-exec-ws-ch (chan (a/dropping-buffer 1))] ;not implemented
    (make-router to-router-ch mg-router-ch router-mg-ch to-ob-ch to-quote-ws-ch error-ch venue symb)
    (mg/make-manager router-mg-ch ob-mg-ch mg-router-ch to-ob-ch to-quote-ws-ch error-ch venue symb)
    (ob/make-orderbook to-ob-ch ob-mg-ch to-exec-ws-ch mg-router-ch)
    (println (str "services started for venue " venue ", symbol " symb))))
 
(defn make-venues [venue-symb-map] ;takes a map from venues to vectors of symbols
  (let [venue-ch-seq
        (mapcat (fn [[venue symbs]]
                  (map (fn [symb]
                         [[venue symb] (chan 100)])
                       symbs))
                (seq venue-symb-map))
        venue-ch-map (reduce (fn [res [k ch]]
                               (assoc res k ch))
                             {}
                             venue-ch-seq)]
    (doseq [[[venue symb] ch] venue-ch-map]
      (make-venue venue symb ch))
    venue-ch-map))

(defn make-venue-manager [venue-symb-map]
  (let [venue-ch-map (make-venues venue-symb-map)]
    (go-loop [msg (<! to-venue-manager-ch)]
      (let [{venue 3 symb 4} msg
            ch (venue-ch-map [venue symb])]
        (>! ch msg)
        (recur (<! to-venue-manager-ch))))))

(defn -main
  [& [port venue symb]]
  (if (and venue symb)
    (make-venue venue symb to-venue-manager-ch)
    (make-venue-manager cf/venues))
  (let [port (Integer. (or port
                           (System/getenv "PORT")
                           5000))]
    (println "all services started")
    (run-server #'app-routes {:port  port
                              :join? false})))
