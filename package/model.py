import json

from sqlalchemy import Column, VARCHAR, Index, create_engine
from sqlalchemy import Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql.schema import ForeignKey

Base = declarative_base()


class Address(Base):
    __tablename__ = 'address'
    id = Column(Integer, primary_key=True)
    category = Column(VARCHAR(500))
    subcategory = Column(VARCHAR(500))
    firm_id = Column(Integer, ForeignKey('firm.id'))
    firm = relationship('Firm')


Index('cat_index', Address.category, Address.subcategory)

class Firm(Base):
    __tablename__ = 'firm'
    id = Column(Integer, primary_key=True)
    name = Column(VARCHAR(1000), index=True)
    address = Column(VARCHAR(1500), index=True)

    coordinates = Column(VARCHAR(1500))
    locality = Column(VARCHAR(500), index=True)

class VisitedLink(Base):
    __tablename__ = 'visited_links'
    id = Column(Integer, primary_key=True)
    link = Column(VARCHAR(1500), index=True)


engine = create_engine("sqlite:///db.sqlite3")
Base.metadata.create_all(engine, checkfirst=True)  # Create what not already there

Session = sessionmaker(bind=engine)
session = Session()

if __name__ == '__main__':
    try:  # Try to pull data that was saved earlier(if any) to avoid extra work
        with open('./viewed.json') as fp:
            downloaded_links = json.load(fp)  # save() will dump old data as well
        with open('./addresses.json') as fp:
            data = json.load(fp)
    except (ValueError, OSError) as e:  # malformed or non-existing = no save data
        pass

    for cat, cat_contents in data.items():
        print('%s:' % cat)
        for subcat, subcatcat_contents in cat_contents.items():
            print('\t%s:' % subcat)
            total = len(subcatcat_contents)
            print('Total: %d' % total)
            session.execute(
                Address.__table__.insert(),
                [
                    {
                        'category': cat,
                        'subcategory': subcat,
                        'name': item['name'],
                        'address': item['address']
                    }
                    for item in subcatcat_contents
                ]

            )
            print()
            session.commit()

    print('links')
    session.execute(
        VisitedLink.__table__.insert(),
        [
            {'link': x}
            for x in downloaded_links
        ]
    )
    session.commit()
