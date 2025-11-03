---
title: Introduction
description: Introduction to Open Compute Framework.
---

# Welcome

**Open Compute Framework** is a framework for decentralized computing. 

## Why Decentralized Computing?

In many cases, a single individual or organization won't have enough resources to run a large-scale computing task. We were facing two main challenges in the past:

* Running LLM inference at a large scale is prohibitively expensive, especially when we need to run many different models on a large benchmark dataset.
* We were hosting a generic benchmark and inviting participants, which exhibits a bursty workload. We need to pay for the idle time when the benchmark is not running.

We believe that decentralized computing can help us solve these problems, in the following ways:

* We can leverage the computing resources from the community, and run the benchmark at a large scale, such that we avoid the cost of running the benchmark on our own. Think about the SETI@home project.
* We avoid single point of failure, as the computing resources are distributed across the globe.
* We avoid the cost of idle time, as we can bring up idle resources to run the benchmark when needed.

## How Does It Work?

The framework is built on top of [LibP2P](https://libp2p.io/), which connects the computing resources in a peer-to-peer network. Each request will be routed to a peer that is able to handle the request. We aim to make the routing as efficient as possible.

The platform also integrates Web3 wallet technology for node owner identification and authentication, enabling decentralized trust and incentive mechanisms.

## Demo

We run a public instance of OCF as the inference API. For more machine learning as a service, please visit [MLaaS](/guides/ml_inference/).