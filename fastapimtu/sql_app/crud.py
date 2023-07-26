from sqlalchemy.orm import Session

from . import models, schemas

def get_history(db: Session, id: int):
    # select * from history where id = 1
    return db.query(models.History).filter(models.History.id == id).first()

def get_all_history(db: Session):
    # select * from history
    return db.query(models.History).all()

def get_history_by_page(db: Session, page: int, size: int):
    # select * from history limit 10 offset 0
    return db.query(models.History).limit(size).offset((page - 1) * size).all()

def create_history(db: Session, history: schemas.History):
    db_history = models.History(query=history.query, result=history.result)
    # insert into history (query, result) values (history.query, history.result)
    db.add(db_history)
    db.commit()
    db.refresh(db_history)
    return db_history