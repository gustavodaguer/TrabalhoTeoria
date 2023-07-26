import json
import threading

from pydantic import BaseModel
from automata.tm.dtm import DTM
from typing import List, Dict
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi_mail import ConnectionConfig, MessageSchema, MessageType, FastMail
from sqlalchemy.orm import Session

import asyncio
import aio_pika
import uvicorn

from sql_app import crud, models, schemas
from sql_app.database import engine, SessionLocal
from util.email_body import EmailSchema
from prometheus_fastapi_instrumentator import Instrumentator

models.Base.metadata.create_all(bind=engine)

conf = ConnectionConfig(
    MAIL_USERNAME="1f48ac17642c9e",
    MAIL_PASSWORD="ed4e357ca57787",
    MAIL_FROM="from@example.com",
    MAIL_PORT=587,
    MAIL_SERVER="sandbox.smtp.mailtrap.io",
    MAIL_STARTTLS=False,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True
)

app = FastAPI()

Instrumentator().instrument(app).expose(app)

class MT(BaseModel):
    input: str
    states: List[str]
    input_symbols: List[str]
    tape_symbols: List[str]
    initial_state: str
    blank_symbol: str
    final_states: List[str]
    transitions: Dict[str, Dict[str, List[str]]]

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def callback(message: aio_pika.IncomingMessage, db: Session = Depends(get_db)):
    async with message.process():
        body = message.body.decode()
        print(f"Received message: {body}")

        mt = json.loads(body)
        dtm = DTM(
            states=set(mt["states"]),
            input_symbols=set(mt["input_symbols"]),
            tape_symbols=set(mt["tape_symbols"]),
            transitions=mt["transitions"],
            initial_state=mt["initial_state"],
            blank_symbol=mt["blank_symbol"],
            final_states=set(mt["final_states"]),
        )
        result = "accepted" if dtm.accepts_input(mt["input"]) else "rejected"

        history = schemas.History(query=str(mt), result=result)
        crud.create_history(db=db, history=history)

        email_schema = EmailSchema(email=["to@example.com"])
        await simple_send(email_schema, result=result, configuration=str(mt))

async def consume_messages():
    connection = await aio_pika.connect_robust("amqp://user:password@rabbitmq/")
    channel = await connection.channel()
    queue = await channel.declare_queue("mt_queue")
    await queue.consume(callback)

@app.post("/send_mt_batch/")
async def send_mt_batch(mts: List[MT]):
    for mt in mts:
        for field in ["states", "input_symbols", "tape_symbols", "initial_state", "blank_symbol", "final_states", "transitions", "input"]:
            if not getattr(mt, field):
                raise HTTPException(status_code=400, detail=f"{field} cannot be empty")

        connection = await aio_pika.connect_robust("amqp://user:password@rabbitmq/")
        channel = await connection.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(mt.dict()).encode()),
            routing_key="mt_queue"
        )

    await asyncio.sleep(2) 

    return {
        "code": "200",
        "msg": "Batch processed successfully"
    }

async def run_consumer():
    await consume_messages()

def start_consumer():
    asyncio.get_event_loop().run_until_complete(run_consumer())

if __name__ == "__main__":
    threading.Thread(target=start_consumer).start()
    uvicorn.run(app, host="0.0.0.0", port=8000)

@app.get("/get_history/{id}")
async def get_history(id: int, db: Session = Depends(get_db)):
    history = crud.get_history(db=db, id=id)
    if history is None:
        return {
            "code": "404",
            "msg": "not found"
        }
    return history


@app.get("/get_all_history")
async def get_all_history(db: Session = Depends(get_db)):
    history = crud.get_all_history(db=db)
    return history


@app.post("/dtm")
async def dtm(info: Request, db: Session = Depends(get_db)):
    info = await info.json()
    states = set(info.get("states", []))

    if len(states) == 0:
        return {
            "code": "400",
            "msg": "states cannot be empty"
        }
    input_symbols = set(info.get("input_symbols", []))
    if len(input_symbols) == 0:
        return {
            "code": "400",
            "msg": "input_symbols cannot be empty"
        }
    tape_symbols = set(info.get("tape_symbols", []))
    if len(tape_symbols) == 0:
        return {
            "code": "400",
            "msg": "tape_symbols cannot be empty"
        }

    initial_state = info.get("initial_state", "")
    if initial_state == "":
        return {
            "code": "400",
            "msg": "initial_state cannot be empty"
        }
    blank_symbol = info.get("blank_symbol", "")
    if blank_symbol == "":
        return {
            "code": "400",
            "msg": "blank_symbol cannot be empty"
        }
    final_states = set(info.get("final_states", []))
    if len(final_states) == 0:
        return {
            "code": "400",
            "msg": "final_states cannot be empty"
        }
    transitions = dict(info.get("transitions", {}))
    if len(transitions) == 0:
        return {
            "code": "400",
            "msg": "transitions cannot be empty"
        }

    input = info.get("input", "")
    if input == "":
        return {
            "code": "400",
            "msg": "input cannot be empty"
        }

    dtm = DTM(
        states=states,
        input_symbols=input_symbols,
        tape_symbols=tape_symbols,
        transitions=transitions,
        initial_state=initial_state,
        blank_symbol=blank_symbol,
        final_states=final_states,
    )
    if dtm.accepts_input(input):
        print('accepted')
        result = "accepted"
    else:
        print('rejected')
        result = "rejected"

    history = schemas.History(query=str(info), result=result)
    crud.create_history(db=db, history=history)

    email_shema = EmailSchema(email=["to@example.com"])

    await simple_send(email_shema, result=result, configuration=str(info))

    return {
        "code": result == "accepted" and "200" or "400",
        "msg": result
    }


async def simple_send(email: EmailSchema, result: str, configuration: str):
    html = """
    <p>Thanks for using Fastapi-mail</p>
    <p> The result is: """ + result + """</p>
    <p> We have used this configuration: """ + configuration + """</p>
    """
    message = MessageSchema(
        subject="Fastapi-Mail module",
        recipients=email.dict().get("email"),
        body=html,
        subtype=MessageType.html)

    fm = FastMail(conf)
    await fm.send_message(message)
    return "OK"
