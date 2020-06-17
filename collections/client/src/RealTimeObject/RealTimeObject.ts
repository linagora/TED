import _ from "lodash"
import Document from "./Document";

export default class RealTimeObject {

    private collectionType: string = "";
    private collectionId: string = "";
    private configuration: RealTimeObjectConfiguration = {};

    constructor(type: string, primaryKey: {
        [key: string]: string
    }){
        this.collectionType = type;
        this.collectionId = JSON.parse(JSON.stringify(primaryKey));
    }

    /**
     * Set Object real time configuration
     * @param options : see RealTimeObjectConfiguration
     */
    public configure(options: RealTimeObjectConfiguration){
        Object.assign(this.configuration, options);
    }

    protected getConfiguration(key: string): any {
       return  _.get(this.configuration, key);
    }

    public subscribe(callback: (objects: Document[]) => any): any {
        //TODO
    }

    public unsubscribe(){
        //TODO
    }

}

type RealTimeObjectConfiguration = {
    "offline"?: {
        "write"?: boolean, //Allow offline write
        "read"?: boolean //Allow offline read
    },
    "undo"?: {
        "allow"?: boolean, //Allow undo / redo function
    }
};