project-root/
│
├── backend/
│   ├── app/
│   │   ├── main.py            # FastAPI entry point
│   │   ├── core/             # Core system logic
│   │   │   ├── config.py     # env vars
│   │   │   ├── security.py   # hashing, JWT, encryption
│   │   │   └── deps.py       # auth dependencies
│   │   │
│   │   ├── db/
│   │   │   ├── session.py    # DB connection
│   │   │   ├── base.py       # Base model
│   │   │   └── init_db.py
│   │   │
│   │   ├── models/           # SQLAlchemy models
│   │   │   ├── user.py
│   │   │   └── anime.py
│   │   │
│   │   ├── schemas/          # Pydantic schemas
│   │   │   ├── user.py
│   │   │   └── anime.py
│   │   │
│   │   ├── api/
│   │   │   ├── routes/
│   │   │   │   ├── auth.py
│   │   │   │   ├── users.py
│   │   │   │   └── anime.py
│   │   │   │
│   │   │   └── api.py        # router include
│   │   │
│   │   ├── services/         # business logic
│   │   │   ├── auth_service.py
│   │   │   └── anime_service.py
│   │   │
│   │   ├── utils/            # helpers
│   │   │   └── encryption.py
│   │   │
│   │   └── middleware/
│   │       └── auth.py
│   │
│   ├── alembic/              # migrations
│   ├── requirements.txt
│   └── .env
│
├── frontend/
│   └── (react / next later)
│
├── nginx/
│   └── default.conf
│
├── docker-compose.yml (optional later)
└── README.md
