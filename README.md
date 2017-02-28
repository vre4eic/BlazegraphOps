# BlazegraphOps
This repository contains the API implementation for the management of Blazegraph Triplestore. There are methods which can be used for the 
import, export and querying of the stored datasets. 

## Blazegraph Management Options
In total, there are implememented three ways of managing Blazegraph. Each one can be used w.r.t. the user's needs and are described next: 

* **Local Access**: when the Blazegraph triplestore is running in the same machine as the repository code. 
* **Remote Access (via Sesame)**: uses the Sesame API for the access and management of Blazegraph. It can be handy  for users which are familiar with the Sesame API. In this case Balzegraph can be installed anywhere. 
* **Access via its restful API**: uses the restful API provided by Blazegraph for the management of the triplestore. As a result, the implemented API which uses the restful API is essentialy a set of clients which use the proper services.  


