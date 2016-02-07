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
              venue "FOREX"
              stock "NYC"]
          (>! to-router-ch [:place-req 
                            (assoc body
                                   :venue venue
                                   :symbol stock)
                            channel])))))

(defn cancel-order [req]
  (with-channel req channel
    (go (>! to-router-ch [:cancel-req (Integer/parseInt (-> req :params :id)) channel]))))

(defn orderbook [req]
  (with-channel req channel
    (go (>! to-router-ch [:orderbook-req "orderbook" channel]))))

(defn quote-req [req]
  (with-channel req channel
    (go (>! to-router-ch [:quote-req "quote" channel]))))

(defn order-status [req]
  (with-channel req channel
    (go (>! to-router-ch [:one-order-status-req (-> req :params :id) channel]))))

(defn all-order-status [req]
  (with-channel req channel
    (go (>! to-router-ch [:all-order-status-req (-> req :params :account) channel]))))

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
  (cc/DELETE "/ob/api/venues/:venue/stocks/:stock/orders/:id/cancel"
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
  (send! ch {:status 200
               :headers {"Content-Type" "application/json; charset=utf-8"}
               :body (json/write-str msg
                                     :key-fn send-key-fn)})
  (close ch))

(defn place-add-fields [req next-id]
  (assoc req
         :id next-id
         :open true
         :ts (get-timestamp)
         :originalqty (req :qty)))

(defn make-router [in-ch mg-router-ch router-mg-ch to-ob-ch]
  ;responsible for maintaining the mapping between response channels and request ids, so
  ;each message goes back to the correct client
  (let [chans [in-ch mg-router-ch]]
    (go-loop [[msg ch] (alts! chans) next-id 0 id-chan {}] ;TODO:  add error chan here
      (cond
        (= ch in-ch) ;new api request, looks like [:req-type request chan]
        (let [[req-type req ch] msg
              new-id-chan (assoc id-chan next-id ch)
              new-next-id (+ next-id 1)]
          (case req-type
            :place-req
            (do (go (>! to-ob-ch [req-type next-id (place-add-fields req next-id)]))
                (recur (alts! chans) new-next-id new-id-chan))
            (:cancel-req :quote-req :orderbook-req :one-order-status-req :all-order-status-req)
            (do (go (>! router-mg-ch [req-type next-id req]))
                (recur (alts! chans) new-next-id new-id-chan))))
        (= ch mg-router-ch) ;response to an api request, looks like [req-id response]
        (let [[req-id resp] msg]
          (send-to-client (id-chan req-id) resp) ;fixes up the json and sends
          (recur (alts! chans) next-id id-chan))))))

        
(defn -main
  [& [port]]
  (let [router-mg-ch (chan 100)
        mg-router-ch (chan 100)
        ob-mg-ch (chan 100)
        to-ob-ch (chan 100)
        to-quote-ws-ch (chan (a/dropping-buffer 1)) ;not implemented
        to-exec-ws-ch (chan (a/dropping-buffer 1))] ;not implemented
    (make-router to-router-ch mg-router-ch router-mg-ch to-ob-ch)
    (mg/make-manager router-mg-ch ob-mg-ch mg-router-ch to-ob-ch to-quote-ws-ch)
    (ob/make-orderbook to-ob-ch ob-mg-ch to-exec-ws-ch mg-router-ch)
    (let [port (Integer. (or port
                             (System/getenv "PORT")
                             5000))]
      (run-server #'app-routes {:port  port
                                :join? false}))))
