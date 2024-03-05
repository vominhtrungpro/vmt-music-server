import asyncio
import websockets
import pymysql

host = 'localhost'
user = 'root'
password = 'tin14091998'
database = 'vmt_music' 

async def select_from_music_queue():
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor
        )
    except pymysql.Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM music_queue WHERE status = '1' ORDER BY updated_at ASC LIMIT 1")
            row = cursor.fetchone()
            if row:
                cursor.execute("UPDATE music_queue SET status = '0' WHERE id = %s", (row['id'],))
                connection.commit()
        return row
    except pymysql.Error as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        connection.close()


async def handler(websocket, path):
    while True:
        try:
            if not websocket.open:
                print("WebSocket not connected, reconnecting...")
                await asyncio.sleep(5)
                continue
        except websockets.exceptions.ConnectionClosedOK:
            print("Connection closed, reconnecting...")
            await asyncio.sleep(5) 
            continue
        except Exception as e:
            print(f"Error: {e}")
            break 
        else: 
            print("Reconnection successful")
            result = await select_from_music_queue()
            if result:
                print(result['url'])
                await websocket.send(result['url'])  

        await asyncio.sleep(10)




start_server = websockets.serve(handler, "localhost", 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
