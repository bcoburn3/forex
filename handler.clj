ns forex.handler
  (:require [forex.mg :as mg]
            [forex.ob :as ob]
            [compojure.core :as cc]
            [compojure.handler :as handler]
            [compojure.route :as route])
  (:use org.httpkit.server) 
  (:gen-class))

(cc/defroutes app-routes
  (cc/GET "/ob/api/heartbeat"
          []
          {:status  200
           :headers {"Content-Type" "application/json; charset=utf-8"}
           :body (json/write-str {"ok" true "error" ""})})
  (cc/POST "/ob/api/venues/FOREX/stocks/NYC/orders"
           {params :params}
           (forex/place-order params))
  (cc/POST "/ob/api/venues/FOREX/stocks/NYC/orders/:id/cancel"
           [id]
           (forex/cancel-order id))
  ;(route/resources "/")
  (route/not-found "Not Found"))

(defn make-router [in-ch from-mg-ch to-mg-ch to-ob-ch]
  ;responsible for maintaing the mapping between response channels and request ids, so
  ;each message goes back to the correct client
  (let [chans [in-ch from-mg-ch]]
    (go-loop [[msg ch] (alts! chan) next-id 0 id-chan {}]
      (cond
        (= ch in-ch) ;new api request, looks like [:type request chan]
        (let [[type req chan] msg
              new-id-chan (assoc id-chan next-id chan)
              new-next-id (+ next-id 1)]
          (case type
            :place-req
            (do (go (>! to-ob-ch [:place-req next-id (place-add-fields req next-id)]))
                (recur (alts! chan) new-next-id new-id-chan))
            :cancel-req
            (do (go (>! to-mg-ch [:cancel-req next-id req]))
                (recur (alts! chan) new-next-id new-id-chan))
            (:quote-req :orderbook-req :one-order-status-req :all-order-status-req)
            (do (go (>! to-mg-ch [type next-id req]))
                (recur (alts! chan) new-next-id new-id-chan))))
        (= ch from-mg-ch)
        (let [[req-id resp] msg]
          (send! (id-chan req-id) msg)
          (recur (alts! chans) next-id id-chan))))))

           
(defn -main
  [& [port]]
  (let [port (Integer. (or port
                           (System/getenv "PORT")
                           5000))]
    (run-server (handler/site #'app-routes {:port  port
                                            :join? false}))))
