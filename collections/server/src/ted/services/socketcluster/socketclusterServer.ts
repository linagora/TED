import socketcluster from "socketcluster-server";
import http from "http";
import { login, tedRequest } from "./procedures";
import handleRequest from "../../core/macroRoutines/RequestHandling";

export let agServer:socketcluster.AGServer;

export function setup(httpServer:http.Server):void
{
    agServer = socketcluster.attach(httpServer);


(async () => {
    for await (let {socket} of agServer.listener('connection')) {
        while(1){
            console.log("new connection");
            /* (async () => { for await (let request of socket.procedure("login")) {
                console.log("login")
                socket.authState = "authenticated";
                request.end();
            }})(); */
    
            for await (let request of socket.procedure("aaa")) {
                console.log("ted");
                if(request.authState !== "authenticated")
                {
                    let notAuthenticatedError = new Error("A socket must be authenticated before accessing TED");
                    notAuthenticatedError.name = "NotAuthenticatedError";
                    request.error(notAuthenticatedError);
                    console.error(notAuthenticatedError);
                    continue;
                }
                try{
                    console.log(request.data.body);
                    let result = await handleRequest(request.data.body, request.data.path);
                    request.end(result);
                }
                catch(err){
                    request.error(err);
                }
            }
        }
    }
})();

}