A data pipeline over Kafka for processing GPS streaming data. There are major components:
1. a (simulating) producer that generates GPS data in the basic format of <vehicleID, lon, lat, timestamp>
2. a stream processor topology that aggregates GPS data by vehicleID in every pre-defined time window to form a GPS ``trace''
3. an OSRM instance receives ``traces'' and perform map-matching
4. a traffic model component that receives the map-matched data, creates a (near) real-time traffice graph and send to a persistence storage (aka. write to disks)


The figures below illustate the data pipeline at **Online** mode and **Offline** mode. Disclaimer: offline mode here is not necessarily 'training mode' of the traffic model.

<img src="images/online_mode_dp.png"  alt = "Online mode" width="800">

Online mode diagram demonstrates the ingestion pipeline of GPS data in stream / real-time. The training process of the traffic model is an acsynchronous process. Specifically, at first,
GPS data is sent / serialized into Kafka from the external provider (e.g., MyTaxi). Then it is sent to a stream processor (Kafka Stream in the current version) for doing 
`windowed aggregation / sampling` to form the "traces" of the vehicle. These `traces'information is then sent to a **routing service** (e.g., OSRM) for map-matching. The map-matching results (which
returned the from-to-speed information) is then used by the traffic model for training / inference.

<img src="images/offline_mode_dp.png"  alt = "Offline mode" width="500">


For offline mode, it is specifically to handle the scenario when the **base map is changed** - that is supposed to be a periodical behavior of base map providers (i.e., HERE, Civil Map, OSM). The change of the base map does not (yet?) preserve the IDs (nodes, edges) of the previous version. In this case, the historical map-matched data is obsoleted and becomes invalid with the new base map. Thus the historical GPS data 
will need to be re-map matched (in batch mode) to be ingested into the traffic model for re-training / inference. 
