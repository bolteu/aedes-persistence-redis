# aedes-persistence-redis

This repository is a fork from [moscajs/aedes-persistence-redis](https://github.com/moscajs/aedes-persistence-redis). 
The reason why the fork was necessary was that the original library had scalability issues. There were two problems:
1. [API function `subscriptionsByTopic`] Every node was keeping a local copy of list of all offline subsciptions (subscriptions by clients with non-clean sessions). This was necessary because sometimes node has to match subscriptions that have wildcards, and the best data structure to do this is a prefix tree (trie). However, Redis does not support such data structure. This was causing problems that:
   1. Bootstrapping node became very slow and took a lot of traffic, as it had to load whole list in memory
   2. Memory requirements for node grows linearly with the number of offline subscriptions, which does not scale.
   
   As we are not using wildcard subscriptions anyways, we decided to simply remove usage of this trie.

2. [API function `createRetainedStream`] When client subscribes, server searches for retained messages on subscribed topics. As subscription can happen by wildcards, we have the similar problem as the previous one. However, instead of keeping a local trie of retained messages, the plugin simply fetches retained messages of all topics and does the matching locally. This approach does not scale either. As we have removed wildcard subscriptions, we have solved this problem by fetching retained messages only for subscribed topics. This was easily achieved, as this information was kept in hashmap in Redis already.
