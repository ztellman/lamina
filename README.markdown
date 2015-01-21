# Lamina is deprecated

Lamina has an enormous amount of functionality, but like many other concurrency libraries (Rx, core.async, etc.), it's a walled garden that doesn't play nicely with others.  In recognition of this, pieces of Lamina have been reimplemented as standalone libraries that can be used with a variety of concurrency primitives and execution models.

* `lamina.core`, `lamina.cache`, `lamina.time` -> [Manifold](https://github.com/ztellman/manifold)
* `lamina.query`, `lamina.stats` -> [Narrator](https://github.com/ztellman/narrator)
* `lamina.executor` -> [Dirigiste](https://github.com/ztellman/dirigiste)
* `lamina.viz` -> nothing, yet
* `lamina.trace` -> nothing, yet

The latest version of [Aleph](https://github.com/ztellman/aleph) is built on this new stack.  Questions about adopting the new libraries can be answered on the [Aleph mailing list](https://groups.google.com/forum/#!forum/aleph-lib).

# Lamina

Lamina is for describing and analyzing streams of data.  It provides a rich set of operators for dealing with these unrealized values, both individually and collectively.  If you're new to Lamina, you should start by reading about [how it deals with individual events](https://github.com/ztellman/lamina/wiki/Introduction).

## Installation

Add the following to the `:dependencies` section of your `project.clj` file:

```clj
[lamina "0.5.6"]
```

## Rationale

Streams of events are represented by channels, which are used in [Aleph](https://github.com/ztellman/aleph) to model network communication over a variety of protocols.  Much like Clojure's sequences, we can apply transforms to all events that pass through the channel:

```clj
> (use 'lamina.core)
nil
> (def ch (channel 1 2 3))
#'ch
> ch
<== [1 2 3 ...]
> (map* inc ch)
<== [2 3 4 ...]
```

Underneath the covers, channels are implemented as a directed graph representing the propagation and transformation of events.  Each new transform we apply adds a node to the graph, and the resulting topology can be rendered with GraphViz.

```clj
> (use 'lamina.core 'lamina.viz)
nil
> (def ch (channel))
#'ch
> (map* inc ch)
<== [...]
> (map* dec ch)
<== [...]
> (enqueue ch 1 2 3)
<< ... >>
> (view-graph ch)
```

![](https://github.com/ztellman/lamina/wiki/images/readme-1.png)

Since we can have multiple consumers for any stream of data, we can easily analyze a stream without affecting other code.  Lamina provides a [variety of operators](http://ztellman.github.com/lamina/lamina.stats.html) for analyzing the data flowing through channels.

Lamina also provides [mechanisms to instrument code](http://ztellman.github.com/lamina/lamina.stats.html), allowing the execution of data to throw off a stream of events that can be aggregated, analyzed, and used to better understand your software.

To learn more, read the [wiki](https://github.com/ztellman/lamina/wiki/Introduction) or the [complete documentation](http://ztellman.github.com/lamina/).  If you have questions, visit the [Aleph mailing list](https://groups.google.com/forum/#!forum/aleph-lib).

# License

Distributed under the Eclipse Public License, which is also used by Clojure.
