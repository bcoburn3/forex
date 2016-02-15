(ns forex.timestamp
  (:require [clj-time.core :as t]
            [clj-time.format :as f]))

(defn get-timestamp []
  (let [formatter (f/formatter "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'")
        cur-mins (/ (System/currentTimeMillis) 1000 60)
        offset-mins (+ (Math/sin (/ cur-mins 3)) 
                       (Math/sin (/ cur-mins 5))
                       (Math/sin (/ cur-mins 7)))
        offset-secs (* offset-mins 60)]
        (f/unparse formatter (t/plus (t/now) (t/seconds offset-secs)))))
