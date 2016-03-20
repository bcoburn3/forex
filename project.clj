(defproject forex "0.5.0"
  :description "clone of www.stockfighter.io server"
  :url "https://github.com/bcoburn3/forex"
  :license {:name "MIT License"
            :url "https://github.com/bcoburn3/forex/blob/master/LICENSE"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/core.async "0.2.374"]
                 [clj-time "0.11.0"]
                 [http-kit "2.1.18"]
                 [compojure "1.4.0"]
                 [ring "1.4.0"]]
  :aot :all
  :main forex.handler
  :uberjar {:aot :all})
