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


    public async topic_create(topic_name:ITopicConfig){
        const admin = this.connection.admin()
        await admin.connect()
        await admin.createTopics({
            topics        : [ topic_name ],
            waitForLeaders: true,
        });
        admin.disconnect()
    }
    public async delete_topic(topics:string[]){
        const admin = this.connection.admin()
        await admin.connect()
        await admin.deleteTopics({topics});
        console.log(`Deleted ${topics} successfully`);
        
        admin.disconnect()
    }
}

(async ()=>{

    // const TOPIC_NAME = 'manu_total_lamports_rewards_block'
    const TOPIC_NAME = 'modern_blocks_json'
    const rdr = new SBKafka({
        brokers : ['localhost:9092'],
        clientId: 'sb',
        groupId : 'rt-test'
    })

    await rdr.consumer.connect()
    await rdr.consumer.subscribe({topic:TOPIC_NAME, fromBeginning:true})
    await rdr.consumer.run({eachMessage: async ({ topic, partition, message, heartbeat }) => {console.log(message)}})

    // await rdr.consumer.run({
    //         eachBatchAutoResolve: true,
    //         eachBatch           : async ({
    //             batch,
    //             resolveOffset,
    //             heartbeat,
    //         }) => {
    //             for (let message of batch.messages) {
    //                 count += 1
    //                 console.log(message.value)
    //                 resolveOffset(message.offset)
    //                 await heartbeat()
    //             }
    //             if ( count>=2000 ){
    //                 console.log("Closing shop");
    //                 rdr.consumer.disconnect()
    //             }
    //         },
    //     })

    // rdr.consumer.seek({ topic: TOPIC_NAME, partition: 0, 
    //     offset: "0" })


})()






