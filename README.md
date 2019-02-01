A data pipeline over Kafka for processing GPS streaming data. There are major components:
1. a (simulating) producer that generates GPS data in the basic format of <vehicleID, lon, lat, timestamp>
2. a stream processor topology that aggregates GPS data by vehicleID in every pre-defined time window to form a GPS ``trace''
3. an OSRM instance receives ``traces'' and perform map-matching
4. a traffic model component that receives the map-matched data, creates a (near) real-time traffice graph and send to a persistence storage (aka. write to disks)


The data pipeline at **Online** mode and **Offline** mode:


