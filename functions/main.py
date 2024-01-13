from firebase_functions import scheduler_fn, options
import firebase_admin
from firebase_admin import firestore, credentials
from firebase_admin.firestore import ArrayRemove, SERVER_TIMESTAMP
import requests
from datetime import datetime as dt
options.set_global_options(max_instances=1)

def send_webhook_notification(message: str) -> None:
    try:
        requests.post("<discord_webhook>",json={
            "content": message
        })
    except Exception as e:
        print(e)
cred = credentials.Certificate('firebase-credentials.json')
firebase_admin.initialize_app(cred)

db = firestore.client()
tasksDocRef = db.collection('index').document('leaderboard_task_queue')
leaderboardDocRef = db.collection('index').document('leaderboard')

@scheduler_fn.on_schedule(schedule='every minute')
def sortleaderboard(event: scheduler_fn.ScheduledEvent) -> None:
    tasks = tasksDocRef.get().to_dict()['jobs']
    if len(tasks) == 0:
        batch = db.batch()
        batch.update(leaderboardDocRef, {'last_updated': SERVER_TIMESTAMP})
        batch.commit()
        print("No Jobs In Queue")
        discord_msg = f"""<t:{int(dt.now().timestamp())}:F>
No jobs in queue.
Updating `last_updated` timestamp.
        """
        send_webhook_notification(discord_msg)
        return

    print("Jobs in queue: ", tasks)
    send_webhook_notification(f"""<t:{int(dt.now().timestamp())}:F>\n{len(tasks)} jobs in queue.\nStarting leaderboard update...""")
    try:
        lb_data = leaderboardDocRef.get().to_dict()['leaderboard']
        jobMap = {}
        oldLBPositionMap = {}
        for i in range(len(lb_data)):
            oldLBPositionMap[lb_data[i]['userId']] = i+1
        for job in tasks:
            jobMap[job['userId']] = {
                "timestamp": (job['timestamp']),
                "newLevel": job["newLevel"],
                "newPoints": job["newPoints"]
            }
        for i in range(len(lb_data)):
            uid = lb_data[i]['userId']
            if(uid in jobMap):
                lb_data[i]['timestamp'] = jobMap[uid]['timestamp']
                lb_data[i]['level'] = jobMap[uid]['newLevel']
                lb_data[i]['points'] = jobMap[uid]['newPoints']
                del jobMap[uid]
                if(len(jobMap) == 0):
                    break
        lb_data.sort(key=lambda x: (-x['level'], x.get('timestamp', 999999999999999999)))
        positionMap = {}
        for i in range(len(lb_data)):
            uid = lb_data[i]['userId']
            if(oldLBPositionMap[uid] != i+1):
                positionMap[uid] = i+1
        
        # Start a batch
        batch = db.batch()

        # Update the leaderboard document
        batch.update(leaderboardDocRef, {'leaderboard': lb_data, 'last_updated': SERVER_TIMESTAMP})
        batch.update(tasksDocRef, {'jobs': ArrayRemove(tasks)})
        for key,value in positionMap.items():
            userDocRef = db.collection('users').document(key)
            batch.update(userDocRef, {'leaderboardPosition': value})
        batch.commit()
        print("Leaderboard Updated")
        discord_msg = f"""<t:{int(dt.now().timestamp())}:F>
Leaderboard updated.
        """
        send_webhook_notification(discord_msg)
    except Exception as e:
        print(e)
        discord_msg = f"""<t:{int(dt.now().timestamp())}:F>
Error updating leaderboard.
Error:
```
{e}
```
        """
