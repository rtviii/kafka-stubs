// const {Kafka}                                = require('kafkajs')
// const {CompressionTypes, CompressionCodecs } = require('kafkajs')

// class Producer {

//         public  brokers : string[] = [];
//         public  clientId: string   = "UninitializedConsumer";
//         private groupId : string   = '';

//         private KafkaConnection:any;

//     public constructor(init?:Partial<Producer>) {
//         Object.assign(this, init);
//     }

//     public async connect_and_write(){

//         const kfk = new Kafka({
//             clientId: this.clientId,
//             brokers : this.brokers
//         })
//         const consumer = kfk.producer()
//         await consumer.connect({ topic: 'rt-test-topic', fromBeginning: true })

//         await consumer.run({
//             eachMessage: async ({ topic, partition, message, heartbeat }:any) => {
//                 console.log({
//                     key: message.key.toString(),
//                     value: message.value.toString(),
//                     headers: message.headers,
//                 })
//             },
//         })

//     }
// }
