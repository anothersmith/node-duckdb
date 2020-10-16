/* eslint-disable @typescript-eslint/no-non-null-assertion */
/* eslint-disable max-classes-per-file */
import {parentPort} from "worker_threads";
import {ConnectionWrapper, ResultWrapperClass} from "./bindings";

const cw = new ConnectionWrapper();
const resultWrapperMap = new Map<number, ResultWrapperClass>();
  
parentPort!.on('message', message => {
    if (message.command === "execute") {
        console.log("-----herexxx")
        resultWrapperMap.set(1, cw.execute(message.query));
        console.log("-----hereyyyy")
        parentPort!.postMessage({event: "executed"})
        console.log("-----herezzzz")
    }

    if (message.command === "fetchRow") {
        parentPort!.postMessage("fetched", <any>resultWrapperMap.get(1)!.fetchRow()) 
    }
})
