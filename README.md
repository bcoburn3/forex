# FOREX
Clone of [Stockfighter](https://www.stockfighter.io) server written in Clojure

## What is it?

The beginnings of a server re-implementation for Stockfighter's trading levels.  Right now it implements all of the 
HTTP Stockfighter trading API, documented at: [https://starfighter.readme.io](https://starfighter.readme.io).

Websockets, position tracking and user will be implemented at some point in the future.

## How do I use it?

The easiest way to try it is:

  1. Install Clojure and Leiningen
  2. Compile everything into one .jar file with 'lein uberjar'
  3. Run with java -jar <jar name here>
  4. Use the API as you usually would at http://localhost:5000

Currently no account verification is implemented, and the server is only minimally tested.
