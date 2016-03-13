(ns forex.config)

(def accounts ;map from accounts to api keys
  {"bcoburn" "123abc" ;should be a long random hex string
   "bots"    "123abd"})

(def venues ;map from venues to a vector of symbols on those venues
  {"FOREX" ["BREQ" "CLJ"]})
