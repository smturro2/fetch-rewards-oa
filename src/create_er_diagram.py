
from sqlalchemy import MetaData, Column, Integer, String, ForeignKey
from sqlalchemy_schemadisplay import create_schema_graph

from database.database import Database

# Note: you will need to have graphviz installed

db = Database()
engine = db.engine
metadata = MetaData()
metadata.reflect(bind=engine)

# Temp add forien keys
# * Receipts.user_id -> Users.id
# * Receipt.brand_code -> Brand.brand_code

metadata.tables['receipts'].append_column(Column('user_id', Integer, ForeignKey('users.id')), replace_existing=True)
metadata.tables['receipts'].append_column(Column('brand_code', String, ForeignKey('brands.brand_code')), replace_existing=True)


# Use graphviz to create the ER diagram
graph = create_schema_graph(engine=db.engine,
                            metadata=metadata,
                            show_datatypes=True,
                            show_indexes=False,
                            rankdir='LR',
                            show_column_keys=True,
                            )

# Save the diagram to a file
filename = '../docs/ER_model.png'
graph.write_png(filename)
