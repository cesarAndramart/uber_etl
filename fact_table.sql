SELECT a.tripId, a.VendorID, c.datetime_id, b.passenger_count_id, f.trip_distance_id, e.rate_code_id, a.store_and_fwd_flag, a.PULocationID, a.DOLocationID, d.payment_type_id, a.fare_amount, a.extra, a.mta_tax, a.tip_amount, a.tolls_amount, a.improvement_surcharge, a.total_amount FROM `psychic-setup-442604-n4.uber_practice.fact_table` a
LEFT JOIN `psychic-setup-442604-n4.uber_practice.passenger_count_dim` b
ON a.passenger_count = b.passenger_count
LEFT JOIN `psychic-setup-442604-n4.uber_practice.datetime_dim` c
ON a.tpep_pickup_datetime = c.tpep_pickup_datetime
AND a.tpep_dropoff_datetime = c.tpep_dropoff_datetime
LEFT JOIN `psychic-setup-442604-n4.uber_practice.payment_type_dim` d
ON a.payment_type = d.payment_type
LEFT JOIN `psychic-setup-442604-n4.uber_practice.ratecodeID_dim` e
ON a.RatecodeID = e.RatecodeID
LEFT JOIN `psychic-setup-442604-n4.uber_practice.trip_distance_dim` f