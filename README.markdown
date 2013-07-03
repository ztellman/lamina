# Lamina

Lamina is for describing and analyzing streams of data.  It provides a rich set of operators for dealing with these unrealized values, both individually and collectively.  If you're new to Lamina, you should start by reading about [how it deals with individual events](https://github.com/ztellman/lamina/wiki/Introduction).

## Installation

Add the following to the `:dependencies` section of your `project.clj` file:

    [lamina "0.5.0-rc3"]
    
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

To learn more, read the [wiki](https://github.com/ztellman/lamina/wiki/Introduction) or the [complete documentation](http://ztellman.github.com/lamina/)

# License

Distributed under the Eclipse Public License, which is also used by Clojure.
