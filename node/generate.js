const { Pool, Client } = require('pg')
const random = require('random');

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'root',
  port: 5432,
})


const trajectories = parseInt(process.argv[2]);
const tposs = new Array(trajectories).fill(1);
const startDate = new Date(Date.UTC(parseInt(process.argv[3]),parseInt(process.argv[4]),parseInt(process.argv[5]),0,0,0))
const endDate = new Date(Date.UTC(parseInt(process.argv[6]),parseInt(process.argv[7]),parseInt(process.argv[8]),0,0,0))
const minVisitsPerDay = parseInt(process.argv[9])
const maxVisitsPerDay = parseInt(process.argv[10])
async function main(){
let venues = await pool.query({
    rowMode: 'array',
    text: 'select venueid from public.categories'
});
//console.log(venues.rows[4][0]);
//return;
let inserts = [];
for (let i= 0;i<trajectories;++i) {
    for (let d = new Date(startDate); d<endDate;d.setDate(d.getDate()+1)){
        let visits = random.uniformInt(minVisitsPerDay,maxVisitsPerDay)();
        d.setMinutes(0);
        d.setHours(0);
        d.setSeconds(0);
        for (let k=0; k<visits;k++){
            //console.log(k);
            const prevd=new Date(d);
            //console.log(d);
            //console.log('|||||||');
            
            do {
              d.setUTCHours(random.uniformInt(prevd.getHours(),Math.floor((k+1)/visits*23))(),random.uniformInt(0,59)(),random.uniformInt(0,59)());
              //console.log((k+1)/visits);
              //console.log(d);
              //console.log("--------------")
              //console.log(prevd);
              //while(true);
            } while (d<prevd);
            //console.log("listo");
            const daux = d.toISOString().replace(/T/, ' ').replace(/\..+/, '');
            //console.log(venues.rowCount);
            const venueid = venues.rows[random.uniformInt(0,venues.rowCount-1)()][0];
            //console.log
            /*const q = await pool.query('insert into public.'+process.argv[11]+'(userid,venueid,utctimestamp,tpos) values ($1, $2, $3, $4)',[i,venueid,daux,tposs[i]],(err,res) => {
                //console.log(err);
                
                    
            });*/
            inserts.push([i,venueid,daux,tposs[i]]);
            tposs[i]++;
            

        }
    }
}

const q = await pool.query('insert into public.'+process.argv[11]+'(userid,venueid,utctimestamp,tpos) values '+inserts.map(v => '('+v[0]+',\''+v[1]+'\',\''+v[2]+'\','+v[3]+')').reduce((acc,val)=>acc+", \n"+val),(err,res) => {
  console.log(err);
  
      
});
//console.log('insert into public.'+process.argv[11]+'(userid,venueid,utctimestamp,tpos) values '+inserts.map(v => '('+v[0]+',\''+v[1]+'\',\''+v[2]+'\','+v[3]+')').reduce((acc,val)=>acc+", \n"+val));
//pool.end()
}
main()

/*pool.query('SELECT venueid from public.categories', (err, res) => {
  console.log(res.rows.map(o => o['venueid']));
  pool.end()
})*/

/*const client = new Client({
  user: 'dbuser',
  host: 'database.server.com',
  database: 'mydb',
  password: 'secretpassword',
  port: 3211,
})
client.connect()

client.query('SELECT NOW()', (err, res) => {
  console.log(err, res)
  client.end()
})*/