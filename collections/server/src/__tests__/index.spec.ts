import {
  saveObject,
  randomObject,
  createdObjects,
  getObject,
} from "./unitTests";
import { writeFile, readFile } from "fs";
import deepEqual from "deep-equal";

async function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function writeAndRead(): Promise<void> {
  let saveOps: Promise<void>[] = [];
  for (let i: number = 0; i < 500; i++) {
    await delay(1000);
    saveOps.push(saveObject(randomObject()));
    //await saveObject(randomObject());
  }

  await Promise.all(saveOps);
  await delay(2000);

  writeFile(
    "src/__tests__/DBState.json",
    JSON.stringify(createdObjects),
    "utf8",
    () => console.log("Done")
  );

  let bool: Boolean = true;
  let allKeys: string[] = [];
  let allObj: Object[] = [];
  await Promise.all(
    Object.entries(createdObjects).map(async ([key, value]) => {
      allKeys.push(key);
      allObj.push(value);
    })
  );
  for (let i: number = 0; i < allObj.length; i++) {
    try {
      let start = new Date().getTime();
      let dbversion = await getObject(allKeys[i]);
      console.log(new Date().getTime() - start);
      console.log(dbversion);
      bool = bool && deepEqual(dbversion, allObj[i]);
    } catch (err) {
      bool = false;
      console.log("============error=========\n", allKeys[i]);
      console.error(err);
    }
  }
  console.log("Success : ", bool);
}
writeAndRead();
