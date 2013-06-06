;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.query.parse
  (:require
    [lamina.time :as t]
    [clojure.string :as str]
    [lamina.query.core :as c])
  (:import
    [java.util.regex Pattern]))

;;;

(def ^:dynamic *input*)
(def ^:dynamic *offset* 0)
(def ^:dynamic *comparator-regex* nil)

(defn raise
  ([s]
     (raise 0 s))
  ([offset msg]
     (throw
       (Exception.
         (str "\n" *input* "\n"
           (apply str (repeat (+ *offset* offset) " ")) "^\n"
           msg)))))

;;;

(defn parse-lookup [s]
  (if (re-find #"\." s)
    (->> (str/split s #"\.")
      (map keyword)
      (list* 'get-in))
    (keyword s)))

(defn token
  ([x]
     (if (fn? x)
       x
       (token x identity)))
  ([x parser]
     (if (fn? x)
       (fn [s]
         (when-let [[v s] (x s)]
           [(parser v) s]))
       (let [s (if (string? x)
                x
                (.pattern ^Pattern x))
             p (Pattern/compile (str "^(?:" s ")"))]
         (fn [^String s]
           (when-let [match (first (re-seq p s))]
             [(parser match) (.substring s (count match))]))))))

(defmacro deftoken
  ([name & params]
     `(def ~name (token ~@params))))

;;;

(defn route [& key-vals]
  (let [pairs (partition 2 key-vals)
        patterns (map first pairs)
        tokens (map second pairs)
        matchers (map token patterns)
        lookup (zipmap matchers tokens)]
    (fn [s]
      (if-let [[m s] (some
                       (fn [m]
                         (when-let [[_ s] (m s)]
                           [m s]))
                       matchers)]
        ((lookup m) s)
        (raise (apply str "Expected one of " (->> patterns (map pr-str) (interpose ", "))))))))

(defn ignore [p]
  (let [t (token p)]
    (fn [s]
      (if-let [[_ s] (t s)]
        [::ignore s]))))

(defn expect [p]
  (let [t (token p)]
    (fn [s]
      (if-let [[_ s] (t s)]
        [::ignore s]
        (raise (str "Expected " p))))))

;;;

(defn chain [& tokens]
  (fn [s]
    (let [len (count s)]
      (loop [tokens tokens, remaining s, acc []]
        (if (empty? tokens)
          [(remove #(= ::ignore %) acc) remaining]
          (when-let [[v remaining]
                     (binding [*offset* (+ *offset* (- len (count remaining)))]
                       ((first tokens) remaining))]
            (recur (rest tokens) remaining (conj acc v))))))))

(defn many [token]
  (fn [s]
    (let [len (count s)]
      (loop [remaining s, acc []]
        (if (empty? remaining)
          [acc remaining]
          (if-let [[v remaining]
                     (binding [*offset* (+ *offset* (- len (count remaining)))]
                       (token remaining))]
            (recur remaining (conj acc v))
            [acc remaining]))))))

(defn second* [a b]
  (let [t (chain (ignore a) b)]
    (fn [s]
      (when-let [[v s] (t s)]
        [(first v) s]))))

(defn maybe [t]
  (fn [s]
    (if-let [x (t s)]
      x
      [nil s])))

(defn one-of [& tokens]
  (fn [s]
    (some #(% s) tokens)))

(defn parser [t]
  (fn [s]
    (let [s (str/trim s)]
      (binding [*input* s]
        (if-let [[v remainder] (t s)]
          (if (empty? remainder)
            v
            (raise (- (count s) (count remainder)) "Unexpected characters."))
          (raise "Failed to parse."))))))

;;;

(declare stream)
(declare pair)
(declare operators)

(def ^:dynamic *comparison* nil)

(deftoken time-unit #"d|h|s|ms|m|us|ns")
(deftoken transform-prefix #"\.")
(deftoken stream-prefix #"&")
(deftoken pattern #"[a-zA-Z0-9:_\-\*]*")
(deftoken id #"[_a-zA-Z][a-zA-Z0-9\-_]*")
(def comparison (fn [s] ((token *comparison*) s)))
(deftoken field #"[_a-zA-Z][a-zA-Z0-9\-_\.]*" parse-lookup)
(deftoken number #"[0-9\.]+" read-string)
(deftoken string #"'[^']*'|\"[^\"]\"*" #(.substring ^String % 1 (dec (count %))))
(deftoken whitespace #"[\s,]*")
(deftoken empty-token #"")
(deftoken colon #"[ \t]*:[ \t]*")

(def time-interval
  (token
    (chain number time-unit)
    #(keyword (apply str %))))

(def number-array
  (token
    (chain
      (ignore whitespace)
      (ignore #"\[")
      (chain number (many (second* whitespace number)))
      (ignore whitespace)
      (expect #"\]"))
    (fn [[[a b]]]
      (vec (list* a b)))))

(def tuple
  (token
    (chain
      (ignore whitespace)
      (ignore #"\[")
      (chain field (many (second* whitespace field)))
      (ignore whitespace)
      (expect #"\]"))
    (fn [[[a b]]]
      (list* 'tuple a b))))

(def value-set
  (token
    (chain
      (ignore whitespace)
      (ignore #"\[")
      (chain (one-of number string)
        (many (second* whitespace (one-of number string))))
      (ignore whitespace)
      (expect #"\]"))
    (fn [[[a b]]]
      (set (list* a b)))))

(def relationship
  (token
    (chain
      (ignore whitespace)
      field
      (ignore whitespace)
      comparison
      (ignore whitespace)
      (one-of number string value-set))
    (fn [[a b c]]
      (list b a c))))

(let [t (delay (one-of tuple string number-array pair relationship operators field time-interval number stream))]
  (defn param [s]
    (@t s)))

(def pair
  (token
    (chain
      id
      colon
      param)
    (fn [[k _ v]]
      {(keyword k) v})))

(def params
  (token
    (chain
      (ignore whitespace)
      (maybe param)
      (many (second* whitespace param)))
    (fn [[a b]]
      (if a
        (list* a b)
        b))))

;;;

(def operator
  (token
    (chain id
      (route
        #"\(" (token (chain params (ignore whitespace) (expect #"\)\.?")) first)
        #"\.|" (token empty-token (constantly ::none))))
    (fn [[name options]]
      (if (= ::none options)
        (keyword name)
        (list* (symbol name) options)))))

(letfn [(selector [target]
          (fn [operator]
            (and (sequential? operator)
                 (= target (first operator)))))]
  (def group-by? (selector 'group-by))
  (def collapse? (selector 'collapse)))

(defn keywordize-facet [operator]
  (if (group-by? operator)
    (let [[op facet & options] operator]
      `(~op ~(if (string? facet)
               (keyword facet)
               facet)
            ~@options))
    operator))

(defn parse-groups
  "Nests operators after a group-by within the group-by operator itself, stopping when it sees a
  collapse operator. Note that collapse is a pseudo-operator; it is used only to punctuate group-by,
  and is not callable itself.

  Returns a pair:
  - The result of processing to the next collapse operator, or end of string
  - The remaining operators to process, including the collapse operator that closed the group-by."
  [s]
  (if (empty? s)
    [nil nil]
    (let [[before [op :as more]] (split-with (complement (some-fn group-by? collapse?)) s)]
      (cond (nil? op) [before nil]
            (collapse? op) [before more]
            (group-by? op) (let [[inside outside] (parse-groups (rest more))
                                 [after remainder] (parse-groups (rest outside))]
                             [(concat before
                                      (list (concat op [(vec inside)]))
                                      after)
                              remainder])))))

(defn collapse-group-bys [s]
  (let [[result remainder] (parse-groups (map keywordize-facet s))]
    (when (seq remainder)
      (throw (IllegalArgumentException.
              "Unexpected collapse without group-by")))
    result))

;;;

(def operators
  (token
    (chain transform-prefix (many operator))
    (fn [[_ operators]]
      (vec (collapse-group-bys operators)))))

(def stream
  (token
    (chain
      stream-prefix
      pattern
      (route
        #"\." (many operator)
        #"" (token empty-token (constantly nil))))
    (fn [[_ pattern operators]]
      (vec (list* pattern (collapse-group-bys operators))))))


;;;

(defn parse-string-query [q]

  (when-not (re-find #"^[&\.]" q)
    (throw
      (IllegalArgumentException.
        "queries must start with either '&' or '.'")))
  
  (binding [*comparison* (->> c/comparators
                           keys
                           (map #(Pattern/quote %))
                           (interpose "|")
                           (apply str)
                           (Pattern/compile))]
    (->> q
      str/split-lines
      (map str/trim)
      (apply str)
      ((parser (one-of stream operators)))
      doall)))



