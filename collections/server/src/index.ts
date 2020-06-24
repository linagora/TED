import * as http from "http";
import createOperation from "./CQLOperations";
import { type } from "os";

 //=================================================
 //                TEST CODE

async function getHTTPBody(req:any):Promise<any>
{
  return new Promise((resolve, reject) => 
  {
    try
    {
      let body:any[] = [];
      let body_str:string;
      req.on('data', (chunk:any) => 
      {
        body.push(chunk);
      }).on('end', () => 
      {
        body_str = Buffer.concat(body).toString();
        resolve(JSON.parse(body_str));
      });
    }
    catch
    {
      reject(new Error("failed parsing HTTP body"));
    }
  }) 
}

http.createServer(async function(req: any, res: any)
{
  console.log("\n\n ===== New Incoming Request =====");
  let body_str = await getHTTPBody(req);
  console.log("Request body =\n",body_str);
  try
  {
    let op=createOperation(body_str);
    op.execute()
    .then( (result:any) => 
    {
      res.write(JSON.stringify(result));
      res.end();
    })
    .catch((err:any) => 
    {
      console.log("catch1", JSON.stringify(err));
      res.write(JSON.stringify(err));
      res.end();
    });
  }
  catch(err)
  {
    console.log("catch2 \n",err);
    res.write('{"status":"' + err.toString() + '"}');
    res.end();
  }
  

}).listen(8080);

console.log("This is a highway to hell")