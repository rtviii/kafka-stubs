import { Kafka}                                from 'kafkajs'
import { CompressionTypes, CompressionCodecs } from 'kafkajs'
const TOPIC_NAME = 'rt-test-topic';


class SBConsumer {

        public brokers : string[] = [];
        public clientId: string   = "UninitializedConsumer";
        private groupId : string = '';

        private KafkaConnection:any;

    public constructor(init?:Partial<SBConsumer>) {
        Object.assign(this, init);
    }

    public async connect_and_read(){

        const kfk = new Kafka({
            clientId: this.clientId,
            brokers : this.brokers
        })

        const consumer = kfk.consumer({ groupId: 'rt-test' })
        await consumer.subscribe({ topic: 'historic_blocks_multipartition'})
        console.log("Connected successfully!");

        await consumer.run({ eachMessage: async ({ topic, message }) => console.log(message.offset)})
        await consumer.seek({ topic: 'historic_blocks_multipartition', partition: 0, offset: "20000" })

        // await consumer.run({
        //     eachMessage: 
        //         async ({ topic, partition, message, heartbeat }:any) => {
        //             console.log({
        //                 key    : message.key.toString(),
        //                 value  : message.value.toString(),
        //                 headers: message.headers})
        //     },
        // })

    }
}



let cons = new SBConsumer({
    brokers:['localhost:9092'], 
    clientId:'sb'}).connect_and_read()

// ( async ()=>{

//     const kfk = new Kafka({
//         clientId: "sb",
//         brokers : ['localhost:9092']
//     })

//     const producer = kfk.producer()
//     await producer.connect()
//     await producer.send({
//         topic:TOPIC_NAME,
//         messages:[
//             { value:"trying zstd compression." }
//         ]
//     })

//     await producer.send({
//     topic      : TOPIC_NAME,
//     compression: CompressionTypes.ZSTD,
//     messages   : [
//         { key: 'key1', value: 'hello world' },
//         { key: 'key2', value: 'hey hey!' }
//     ],
//     })
//     await producer.disconnect();
//     console.log("Disconnected producer successfully.");

// })()

