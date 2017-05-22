Summary of Changes
==================

Editor Comments
---------------
> We have received three review reports. While all the reviewers appreciate your research efforts, a number of concerns on the novelty, performance bound, query accuracy, experiments, etc. have been raised. Please address all these concerns in a major revision.

(response)


Reviewer: 1
-----------
> This paper proposes a novel distributed sketch, Synopsis, to index spatiotemporal stream data. Synopsis is update friendly. Specifically, Synopsis can scale effectively when the arrive rate of the data stream is faster than the rate at which the Synopsis can be updated. Synopsis is a summary of the data but most information is reserved such that queries on Synopsis can produce results of high accuracy.

(response)

> 1) In Section 3, the techniques of different parts of Synopsis are proposed. But I feel the algorithms are not very clearly proposed. It is better to present the algorithms in the pseudo code manner.

(response)

> 2) The theoretical performance analysis of the techniques is not discussed. Maybe it is better to propose some bounds of the techniques of Synopsis, e.g., bound of time complexity or bound of error.

(response)

> 3) For experiments, just one dataset is used in experiments. More datasets should be introduced to study the performance of Synopsis extensively.

(response)

> 4) Synopsis should be compared with existing works in experiments to more clearly show the advantages of Synopsis. It is better to pick the state-of-the-art existing technique and make comparison in experiments.

(response)

> 5) In Section 4.6, just the random query is studied. It is better to use some real queries.

(response)

> 6) The experiment of tuning the number of machines of the cluster should be added. It is an important experiment to show the scalability of a distributed technique.

(response)

> 7) I am confused by Table 4. I think using full data should be more accurate than using a subset of the full data. But, Table 4 shows that the RMSE of using full data is higher than the RMSEs of using 10% and 20% of the full data.

(response)


Reviewer: 2
-----------

> Strong points:
> (1) The problem of processing general queries over spatio-temporal observational data is quite useful and practical;
> (2) Based on the experimental results, the proposed solutions are efficient.

(response)

> Weak points:
> (1) The presentation can be improved. The overflow of this paper is not easy to follow;
> (2) The technical contributions and challenges of the problem studied in this work are not presented in a clear way.

(response)

> (1) In Section 1, the first and second items in “challenges” are similar. High data arrival rates will incur high data volumes. Consequently, challenges of “data volumes” and “data arrival rates” can be merged;

Thank you for bringing this to our attention. We have merged the first two challenges.

> (2) The overflow is not easy to follow. The format/definition of the queries to be handled is still unclear. The problem definition (i.e., the format of the observational streaming data, the definition of the queries, etc.) can be presented before Section 2;

(response)

> (3) In Section 3.3, what are the novelty and technical challenge(s) of “sketchlet”, especially in the context of “distributed maintenance”?

(response)

This manuscript focuses on the problem of processing queries over a stream of spatio-temporal observational data. Each item in the stream contains a geographical location, a timestamp, and a set of key-value pairs. The authors develop a number of components to efficiently process the queries. Experimental results demonstrate that their proposal is capable of high efficiency. Overall, this is a good piece of research work that studies an interesting and practical problem. But it can be improved, especially in terms of presentation.


Reviewer: 3
-----------

> This paper proposes a distributed sketch over spatiotemporal streams called SYNOPSIS. This sketch maintains a compact representation of the streaming data, organized as a so-called SIFT structure, and it supports dynamic scaling to preserve responsiveness and avoid overprovisioning. A set of queries are supported by the proposed sketch, such as relational queries, statistical queries, etc. The experimental study demonstrates the efficacy of SYNOPSIS.

(response)

> My major concern is that the core technique of the sketch is based on the previous work [11], and a set of queries can be supported is because of the usage of Welford's method [11], and thus the novelty is limited, although the authors take into account the varied data density and arrival rates. I think the authors need to exploit more novel techniques to support more types of queries (maybe in the future work?).

(response)

> Another question is about the spatial or temporal query window size. The distributed sketch is based on the geohash algorithm, which divides the earth into a hierarchy of bounding boxes. The spatial range specified in the query may not cover the bound boxes exactly, which means that only part of the data in a box should be considered rather than the entire data in the box. However, the record in each box used in query processing contains statistics for the whole box. How can the accuracy of the queries be guaranteed?

(response)

> The SIFT structural compaction is not described clearly enough. I suggest the authors add an example in that section. Is it true that it does not matter how the original SIFT is constructed (either spatial first level or temporal first level), because it will be reconfigured dynamically?

(response)

> Yufei Tao et al. proposed a sketch-based method for spatio-temporal aggregation (Spatio-Temporal Aggregation Using Sketches. ICDE'04), which is relevant to this work. The authors should discuss this paper in the related work section.

(response)