# flint

A *lighter* database

## Architecture

## Source Layout
```
  Server
    └─> Handler
          └─> Executor
                ├─> Parser (SQL → AST)
                ├─> Planner (AST → Plan)
                ├─> Storage Engine (data)
                └─> execute_plan(Plan, Storage) → Response
```