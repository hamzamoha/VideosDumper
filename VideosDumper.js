import { Storage } from "@google-cloud/storage";
import axios from "axios";
import { } from 'dotenv/config'
import mysql from "mysql"
import _ from "lodash";
import cron from 'node-cron';

function new_connection() {
    return mysql.createConnection({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME,
    });
}

class TiktokVideo {
    username;
    id;
    title;
    url;
    date;
    gcs_url;
    uploaded = false;

    constructor(username, id, url, title, date) {
        this.username = username;
        this.id = id;
        this.date = date;
        this.title = this.addslashes(encodeURIComponent(title)).substring(0, 1000);
        this.url = url;
        this.gcs_url = `${process.env.GCS_HOME}${process.env.GCS_BUCKET}/videos-tiktok/${this.username}/${this.id}.mp4`;
    }

    get_GCS_URL() {
        return this.gcs_url;
    }

    getTitle() {
        return this.title;
    }

    isUploaded() {
        return this.uploaded;
    }

    async upload(bucket) {
        const file = await bucket.file(`videos-tiktok/${this.username}/${this.id}.mp4`);
        let id = this.id
        let url = this.url
        let this_ref = this;
        await file.exists().then(async function (data) {
            const exists = data[0];
            if (!exists) {
                const writeStream = await file.createWriteStream();
                await axios({
                    url: url,
                    method: 'GET',
                    responseType: 'stream'
                })
                    .then(async (res) => {
                        await res.data.pipe(writeStream)
                            .on('error', async () => {
                                console.log("Error while uploading: " + id + " !");
                            })
                            .on('finish', async () => {
                                console.log(id + ' uploaded')
                                this_ref.store();
                            });
                    })
                    .catch(async (error) => {
                        console.log("Error Getting Video From Url !");
                    });
            }
            else {
                console.log(id + " Already Uploaded !")
                this_ref.store();
            }
        });
    }

    async store() {
        let connection = new_connection();
        let id = this.id;
        let username = this.username;
        let title = this.title;
        let d = new Date(Number(this.date) * 1000);
        let date = d.toISOString().split('T')[0] + ' ' + d.toTimeString().split(' ')[0];
        await connection.query(`SELECT count(*) count FROM ${process.env.DB_TABLE_VIDEOS} WHERE video_id = '${id}'`,
            async (error, res) => {
                if (error) {
                    console.log(error.sqlMessage);
                    connection.end()
                }
                else if (!res) {
                    console.error("Error While Fetching Result");
                    connection.end()
                }
                else {
                    if (res[0].count == 0) {
                        await connection.query(`INSERT INTO ${process.env.DB_TABLE_VIDEOS} (pseudo, video_id, title, date) VALUES ('${username}', '${id}', '${title}', '${date}')`,
                            function (error) {
                                if (error) {
                                    console.error("Error While Storing The Video", error.sqlMessage);
                                }
                                else {
                                    this.uploaded = true
                                    console.log(id + " stored")
                                }
                                connection.end()
                            }
                        );
                    }
                    else {
                        await connection.query(`UPDATE ${process.env.DB_TABLE_VIDEOS} SET date = '${date}' WHERE video_id = '${id}'`,
                            function (error) {
                                if (error) {
                                    console.error("Error While Updating The Date ! ", error.sqlMessage);
                                }
                                else {
                                    this.uploaded = true
                                    console.log(id + " Date Updated")
                                }
                                connection.end()
                            }
                        );
                        // console.log(id + " ALready Stored !");
                        // connection.end()
                    }
                }
            }
        );
    }

    async save(bucket) {
        await this.upload(bucket)
    }

    addslashes(string) {
        return string.replace(/\\/g, '\\\\').
            replace(/\u0008/g, '\\b').
            replace(/\t/g, '\\t').
            replace(/\n/g, '\\n').
            replace(/\f/g, '\\f').
            replace(/\r/g, '\\r').
            replace(/'/g, '\\\'').
            replace(/"/g, '\\"');
    }
}

class VieosDumper {
    username;
    tiktok_url;
    gcs_url;
    max_cursor = 0;
    videos = new Array();
    options;
    connection;
    bucket;
    max_results = 50;
    counter = 0;

    constructor(username, bucket, max_results = 50) {
        if (Number(max_results) && Number(max_results) > 0) this.max_results = Number(max_results);
        this.connection = new_connection();
        this.username = username;
        this.bucket = bucket;
        this.tiktok_url = `${process.env.TIKTOK_HOME}@${username}`;
        this.gcs_url = `${process.env.GCS_HOME}${process.env.GCS_BUCKET}/videos-tiktok/${this.username}/`;
        this.options = {
            method: "GET",
            url: `${process.env.TIKTOK_API_URL}/user/${this.username}/feed`,
            headers: {
                "x-rapidapi-host": process.env.TIKTOK_API_HEADERS_HOST,
                "x-rapidapi-key": process.env.TIKTOK_API_HEADERS_KEY,
            },
        };
    }

    async response_callback(response) {
        let has_more = Number(response.data.data.has_more);
        let vids = response.data.data.aweme_list;
        let this_ref = this;
        for await (let v of vids) {
            if (this.counter < this.max_results) {
                this.counter++;
            }
            else {
                console.log("All " + this.max_results + " Are Uploaded and Stored !");
                await this.connection.end();
                return
            }
            let url;

            if (v["video"]["play_addr_h264"]["url_list"]) url = v["video"]["play_addr_h264"]["url_list"][0];

            else if (v["video"]["play_addr"]["url_list"]) url = v["video"]["play_addr"]["url_list"][0];

            else url = v["video"]["download_addr"]["url_list"][0];

            await this.push_video(v.aweme_id, url, v.desc, v.create_time);
        }
        if (has_more == 1) {
            console.log("There's more (" + this.username + ")");
            let connection = this.connection;
            let username = this.username;
            let max_cursor = this.max_cursor = Number(response.data.data.max_cursor);
            await connection.query(`DELETE FROM ${process.env.DB_TABLE_CURSOR} WHERE pseudo = '${username}'`,
                async (error, result) => {
                    if (error) {
                        console.error(error.sqlMessage);
                    }
                    await connection.query(`INSERT INTO ${process.env.DB_TABLE_CURSOR} (pseudo, max_cursor) VALUES ('${username}', '${Number(response.data.data.max_cursor)}')`,
                        async function (error, result) {
                            if (error) {
                                console.log(error.sqlMessage);
                            }
                            else {
                                console.log(`Cursor Updated`);
                            }
                        }
                    );
                }
            );
            setTimeout(async () => {
                await this_ref.resume_dumping(Number(response.data.data.max_cursor));
            }, 60000);
            return true;
        }
        else {
            let videostring = (this.counter == 1) ? " video" : " videos";
            console.log("No more videos found ! (" + this.username + ") We got " + this.counter + videostring + "!");
            await this.connection.end();
            return false
        }
    }

    async start_dumping() {
        if (this.options.params) this.options.params = null;
        await axios
            .request(this.options)
            .then(async (response) => await this.response_callback(response))
            .catch(async (error) => {
                console.error("Error While Using the API");
            });
    }

    async resume_dumping(max_cursor = 0) {
        var this_ref = this;

        if (max_cursor == 0) {
            return this_ref.start_dumping();
        }
        else {
            this_ref.max_cursor = max_cursor;
            this_ref.options.params = {
                max_cursor: max_cursor
            };
            await axios
                .request(this_ref.options)
                .then(async (response) => this_ref.response_callback(response))
                .catch(async (error) => {
                    console.error("Error While Using the API");
                });
        }


    }

    async setCursor(error, result) {
        if (error) {
            console.error(error.sqlMessage);
        }
        await result.forEach(async (row) => {
            this.max_cursor = Number(row.max_cursor) ? Number(row.max_cursor) : 0;
        });
    }

    async setup_max_cursor() {
        if (this.max_cursor > 0) return true;
        await this.connection.query(`SELECT max_cursor FROM ${process.env.DB_TABLE_CURSOR} WHERE pseudo = '${this.username}'`,
            async (error, result) => await this.setCursor(error, result));
        if (this.max_cursor == 0) return false;
        return true;
    }

    async push_video(video_id, video_url, video_title, video_date) {
        let i = this.videos.push(new TiktokVideo(this.username, video_id, video_url, video_title, video_date));
        await this.videos[i - 1].save(this.bucket);
    }

    getTikTokURL() {
        return this.tiktok_url;
    }

    get_GCS_URL() {
        return this.gcs_url;
    }
}

class Main {
    storage;
    bucket;
    constructor() {
        this.storage = new Storage({
            projectId: process.env.GCS_PROJECTID,
            keyFilename: process.env.GCS_KEY_JSON
        });
        this.bucket = this.storage.bucket(process.env.GCS_BUCKET);
    }

    async start(max_results) {
        let conn = new_connection();
        let bucket = this.bucket;
        await conn.query(`SELECT url FROM ${process.env.DB_TABLE_USERS}`, async function (error, result) {
            if (error) {
                console.error(error.sqlMessage);
            }
            await result.forEach(async function (row) {
                let username = await row.url.split('/').reverse()[0].substring(1);
                let videosDumper = new VieosDumper(username, bucket, max_results);
                await videosDumper.resume_dumping();
            });
            await conn.end();
        });
    }
}


let main = new Main()




cron.schedule('*/15 * * * *', () => {
    main.start(50);
});

