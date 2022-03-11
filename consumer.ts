import { Admin           , Consumer         , ITopicConfig, Kafka, Producer} from 'kafkajs'
import { CompressionTypes, CompressionCodecs, KafkaMessage                               } from 'kafkajs'






class  SBKafka{
    /**
     * -----------------------------------------------------
     * General purpose kafka interactions
     * -----------------------------------------------------
     */

    public    brokers   : string[];
    public    clientId  : string;
    public    groupId   : string;

    private connection: Kafka;

    
    public consumer: Consumer; //https://kafka.js.org/docs/consuming#a-name-options-a-options
    public producer: Producer;
    public admin   : Admin;

    public constructor(init:Pick<SBKafka, "brokers"|"clientId" | "groupId">) {
        Object.assign(this, init);
        this.connection = new Kafka({
            clientId: this.clientId,
            brokers : this.brokers
        })

        this.consumer = this.connection.consumer({ groupId: init.groupId, maxWaitTimeInMs:200})
        this.producer = this.connection.producer();
        this.admin    = this.connection.admin();
    }


    public async get_msgs_from_topic(topicName:string, count:number=10, from_offset:number=-1,partition:number=0):Promise<Array<Buffer|null>>{

            const consumer            = this.consumer
            let   {high, low, offset} = (await this.admin.fetchTopicOffsets(topicName)).filter(partition_obj => partition_obj.partition ===partition)[0]
            const accumulator:Array<Buffer|null> = []

            consumer.connect()
            consumer.subscribe({topic:topicName, fromBeginning: from_offset <0  ? true : false})
            
        await consumer.run({
            eachMessage: async ({ message }) => {
                            console.log("Got count", count);
                            count = count - 1
                            console.log("----count is now ",count);
                            accumulator.push(message.value);
                            count < 1 ? consumer.disconnect() : ""
            }
        })

        consumer.seek({ topic: topicName, partition, offset: low.toString()})
        while ( count > 0 ){
            setTimeout(()=>{console.log('looping...');},1000)

        }


        console.log(accumulator.length);
        console.log("GOT OFFFSETS", {high, low, offset});

        return accumulator
    }


}

(async ()=>{

    const TOPIC_NAME = 'modern_blocks_json'
    const rdr = new SBKafka({
        brokers : ['localhost:9092'],
        clientId: 'sb',
        groupId : 'rt-test'
    })


    let messages = await rdr.get_msgs_from_topic(
        TOPIC_NAME,1
        
    )

    console.log("Got ,messages ", messages.length);
    
})()
// (async ()=>{



//     // const TOPIC_NAME = 'manu_total_lamports_rewards_block'
//     const TOPIC_NAME = 'modern_blocks_json'
//     const rdr = new SBKafka({
//         brokers : ['localhost:9092'],
//         clientId: 'sb',
//         groupId : 'rt-test'
//     })

//     const { HEARTBEAT, FETCH_START } = rdr.consumer.events
//     const removeListener1            = rdr.consumer.on(HEARTBEAT, e => console.log(`heartbeat at ${e.timestamp}`))
//     const removeListener2            = rdr.consumer.on(FETCH_START, e => console.log(`Started fetching data at ${e.timestamp}`))
//     await rdr.admin.connect()
//     await rdr.consumer.connect()
//     await rdr.consumer.subscribe({topic:TOPIC_NAME, fromBeginning:true})
//     await rdr.consumer.run({eachMessage: async ({ topic, partition, message, heartbeat }) => {
//         // console.log(message.value?.toString())
//     }})

//     // let x=  await rdr.admin.listTopics()
//     // let y =  await rdr.admin.fetchTopicMetadata()

//     const offsets = await rdr.admin.fetchTopicOffsets(TOPIC_NAME)
//     console.log(offsets[0].offset);

    
//     // let count = 0
//     // await rdr.consumer.run({
//     //         eachBatchAutoResolve: true,
//     //         autoCommit          : true,
//     //         eachBatch           : async ({
//     //             batch,
//     //             resolveOffset,
//     //             heartbeat,
//     //         }) => {
//     //             for (let message of batch.messages) {
//     //                 count += 1
//     //                 console.log(message.value)
//     //                 resolveOffset(message.offset)
//     //                 await heartbeat()
//     //             }
//     //             if ( count>=2000 ){
//     //                 console.log("Closing shop");
//     //                 rdr.consumer.disconnect()
//     //             }
//     //         },
//     //     })

//     // rdr.consumer.seek({ topic: TOPIC_NAME, partition: 0, offset: "0" })


// })()






