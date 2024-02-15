from kafka import KafkaConsumer



import json


if __name__ == "__main__":
    #get consumer instance 

    consumer= KafkaConsumer(
        "registered_user",
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest', #it start consume when first get it to partitions
        group_id="consumer-group-a"
    )
    print("starting consumer")
    for msg in consumer:
        print("Registered User = {}".format(json.loads(msg.value)))