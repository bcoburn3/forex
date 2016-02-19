(ns forex.handler
  (:require [forex.manager :as mg]
            [forex.orderbook :as ob]
            [forex.timestamp :as ts :refer [get-timestamp]]
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

(def to-router-ch (chan 100))

(defn place-order [req]
  (with-channel req channel
    (go (let [body (json/read-str (slurp (-> req :body))
                                  :key-fn (fn [x] (keyword (clojure.string/lower-case x))))
              venue (-> req :params :venue)
              symbol (-> req :params :stock)]
          (>! to-router-ch [:place-req 
                            (assoc body
                                   :venue venue
                                   :symbol symbol
                                   :price (if (= (body :ordertype) "market") 0 (body :price)))
                            channel venue symbol])))))

(defn cancel-order [req]
  (let [venue (-> req :params :venue)
        symbol (-> req :params :stock)]
    (with-channel req channel
      (go (>! to-router-ch [:cancel-req (Integer/parseInt (-> req :params :id)) channel
                            venue symbol])))))

(defn orderbook [req]
  (let [venue (-> req :params :venue)
        symbol (-> req :params :stock)]
    (with-channel req channel
      (go (>! to-router-ch [:orderbook-req "orderbook" channel venue symbol])))))

(defn quote-req [req]
  (let [venue (-> req :params :venue)
        symbol (-> req :params :stock)]
    (with-channel req channel
      (go (>! to-router-ch [:quote-req "quote" channel venue symbol])))))

(defn order-status [req]
  (let [venue (-> req :params :venue)
        symbol (-> req :params :stock)
        id (-> req :params :id)]
    (with-channel req channel
      (go (>! to-router-ch [:one-order-status-req id channel venue symbol])))))

(defn all-order-status [req]
    (let [venue (-> req :params :venue)
          symbol (-> req :params :stock)
          account (-> req :params :account)]
      (with-channel req channel
        (go (>! to-router-ch [:all-order-status-req account channel venue symbol])))))

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
          [venue account symbol]
          all-order-status)
  (route/not-found "Not Found"))

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
                                     :key-fn send-key-fn)})
  (close ch))

(defn send-error [ch error-code error-text]
  (send! ch {:status error-code
             :headers {"Content-Type" "application/json; charset=utf-8"}
             :body (json/write-str {:ok false
                                    :error error-text})})
  (close ch)
  nil)

(defn place-add-fields [req next-id]
  (assoc req
         :id next-id
         :open true
         :ts (get-timestamp)
         :originalqty (req :qty)))

(defn validate-order [order ch]
  (if (not (and (order :direction) (order :qty) (order :account) (order :ordertype)))
    (send-error ch 400 "Missing field, one of direction, qty, account, orderType") ;send-error returns nil
    (cond (< (order :qty) 1)
          (send-error ch 400 "qty must be positive")
          (< (order :price) 0)
          (send-error ch 400 "price must be non-negative")
          true true)))

(defn make-router [in-ch mg-router-ch router-mg-ch to-ob-ch error-ch venue symbol]
  ;responsible for maintaining the mapping between response channels and request ids, so
  ;each message goes back to the correct client
  (let [chans [in-ch mg-router-ch error-ch]]
    (go-loop [[msg ch] (alts! chans) next-id 0 id-chan {}] ;TODO:  add error chan here
      (cond
        (= ch in-ch) ;new api request
        (let [[req-type req ch & [r-venue r-symbol]] msg
              new-id-chan (assoc id-chan next-id ch)
              new-next-id (+ next-id 1)]
          (if (not (= r-venue venue))
            (do (send-error ch 404 (str "No venue exists with the symbol " r-venue))
                (recur (alts! chans) next-id id-chan))
            (case req-type
              :place-req
              (do (if (not (= r-symbol symbol)) 
                    (send-error ch 404 (str "symbol " r-symbol " does not exist on venue " venue))
                    (if (validate-order req ch)
                      (go (>! to-ob-ch [req-type next-id (place-add-fields req next-id)]))))
                  (recur (alts! chans) new-next-id new-id-chan))
              (:cancel-req :quote-req :orderbook-req :one-order-status-req)
              (do (if (not (= r-symbol symbol)) 
                    (send-error ch 404 (str "symbol " r-symbol " does not exist on venue " venue))
                    (go (>! router-mg-ch [req-type next-id req])))
                  (recur (alts! chans) new-next-id new-id-chan))
              :all-order-status-req
              (do (go (>! router-mg-ch [req-type next-id req]))
                  (recur (alts! chans) new-next-id new-id-chan)))))
        (= ch mg-router-ch) ;response to an api request
        (let [[req-id resp] msg]
          (send-to-client (id-chan req-id) resp) ;fixes up the json and sends
          (recur (alts! chans) next-id id-chan))
        (= ch error-ch)
        (let [[req-id error-code error-text] msg]
          (send-error (id-chan req-id) error-code error-text)
          (recur (alts! chans) next-id id-chan))))))

(defn -main
  [& [venue symbol port]]
  (let [venue (or venue "FOREX")
        symbol (or symbol "BREQ")
        port (Integer. (or port
                           (System/getenv "PORT")
                           5000))
        router-mg-ch (chan 100)
        mg-router-ch (chan 100)
        ob-mg-ch (chan 100)
        to-ob-ch (chan 100)
        error-ch (chan 100)
        to-quote-ws-ch (chan (a/dropping-buffer 1)) ;not implemented
        to-exec-ws-ch (chan (a/dropping-buffer 1))] ;not implemented
    (make-router to-router-ch mg-router-ch router-mg-ch to-ob-ch error-ch venue symbol)
    (mg/make-manager router-mg-ch ob-mg-ch mg-router-ch to-ob-ch to-quote-ws-ch error-ch venue symbol)
    (ob/make-orderbook to-ob-ch ob-mg-ch to-exec-ws-ch mg-router-ch)
    (println "services started")
    (run-server #'app-routes {:port  port
                              :join? false})))
