import os
from datetime import datetime
from typing import Any, Dict, Optional

import redis
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from qtask_list.dashboard.auth import (
    COOKIE_NAME,
    auth_config_error,
    create_session_token,
    get_auth_settings,
    is_request_authenticated,
    require_auth,
    verify_credentials,
)
from qtask_list.admin import QueueAdmin, QueueState
from qtask_list.archiver import Monitor
from qtask_list.models import DuplicateAction, IdentityPolicy
from qtask_list.storage import RemoteStorage
from qtask_list.security import redis_endpoint_label


app = FastAPI(title="qtask_list Dashboard")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
STORAGE_URL = os.environ.get("QTASK_STORAGE_URL", "")
redis_client: Any = redis.from_url(REDIS_URL, decode_responses=True)
storage = RemoteStorage(STORAGE_URL) if STORAGE_URL else None
admin = QueueAdmin(redis_url=REDIS_URL, redis_client=redis_client, storage=storage)
monitor = Monitor(redis_client)

BASE_DIR = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

# React SPA 构建产物（frontend/ 构建，输出到 static/spa）
SPA_DIR = os.path.join(BASE_DIR, "static", "spa")
SPA_INDEX = os.path.join(SPA_DIR, "index.html")
if os.path.isdir(os.path.join(SPA_DIR, "assets")):
    app.mount(
        "/assets",
        StaticFiles(directory=os.path.join(SPA_DIR, "assets")),
        name="spa_assets",
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class PushTaskRequest(BaseModel):
    payload: Dict[str, Any]
    action: Optional[str] = None
    delay_seconds: int = Field(default=0, ge=0)
    expire_seconds: int = Field(default=0, ge=0)
    logical_key: Optional[str] = None
    scheduled_for: Optional[datetime] = None
    not_before_at: Optional[datetime] = None
    start_deadline_at: Optional[datetime] = None
    dedup_until: Optional[datetime] = None
    trace_id: Optional[str] = None
    parent_task_id: Optional[str] = None
    concurrency_key: Optional[str] = None
    supersede_key: Optional[str] = None
    supersede_version: int | str | None = None
    duplicate_action: DuplicateAction = DuplicateAction.REJECT
    confirm_duplicate: bool = False


class RecoverRequest(BaseModel):
    include_active: bool = False
    confirm_active: bool = False
    worker_id: Optional[str] = None


class ClearQueueRequest(BaseModel):
    include_dlq: bool = True
    include_history: bool = False
    include_processing: bool = False
    confirm_processing: bool = False
    identity_policy: IdentityPolicy = IdentityPolicy.KEEP
    confirm_identity_release: bool = False


class RequeueTaskRequest(BaseModel):
    queue: str
    from_state: QueueState = QueueState.dlq


class RequeueDlqRequest(BaseModel):
    task_id: Optional[str] = None
    confirm_bulk: bool = False


class ReplayTaskRequest(BaseModel):
    queue: Optional[str] = None
    start_deadline_at: Optional[datetime] = None
    payload: Optional[Dict[str, Any]] = None
    logical_key: Optional[str] = None
    replace_payload: bool = False
    replace_logical_key: bool = False
    action: Optional[str] = None


class LoginRequest(BaseModel):
    username: str
    password: str


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    settings = get_auth_settings()
    if not settings.enabled or is_request_authenticated(request):
        return RedirectResponse("/")
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "username": settings.username,
            "config_error": auth_config_error(settings),
        },
    )


@app.get("/api/auth")
def api_auth(request: Request):
    settings = get_auth_settings()
    return {
        "enabled": settings.enabled,
        "authenticated": is_request_authenticated(request),
        "username": settings.username if settings.enabled else None,
        "error": auth_config_error(settings),
    }


@app.post("/api/login")
def api_login(request: LoginRequest, response: Response):
    settings = get_auth_settings()
    config_error = auth_config_error(settings)
    if config_error:
        raise HTTPException(status_code=500, detail=config_error)
    if not settings.enabled:
        return {"authenticated": True, "enabled": False}
    if not verify_credentials(request.username, request.password):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    response.set_cookie(
        COOKIE_NAME,
        create_session_token(settings.username),
        max_age=settings.session_ttl,
        httponly=True,
        secure=settings.secure_cookie,
        samesite="lax",
        path="/",
    )
    return {"authenticated": True, "enabled": True, "username": settings.username}


@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie(COOKIE_NAME, path="/")
    return {"authenticated": False}


@app.get("/api/health")
def api_health(_auth: None = Depends(require_auth)):
    try:
        redis_client.ping()
        mem_info = monitor.get_memory_info()
        return {"status": "ok", "redis": redis_endpoint_label(REDIS_URL), "memory": mem_info}
    except Exception:
        return {"status": "error", "error": "Redis unavailable", "redis": redis_endpoint_label(REDIS_URL)}


@app.get("/api/queues")
def api_queues(_auth: None = Depends(require_auth)):
    return admin.list_queues()


@app.get("/api/workers")
def api_workers(
    queue: Optional[str] = Query(None, description="Queue name"),
    _auth: None = Depends(require_auth),
):
    return admin.list_workers(queue)


@app.get("/api/actions")
def api_actions(
    limit: int = Query(1000, ge=1, le=10000),
    _auth: None = Depends(require_auth),
):
    actions = set()
    scanned = 0
    for key in redis_client.scan_iter("qtask:task:*", count=100):
        task_id = key.replace("qtask:task:", "")
        task = admin.get_task(task_id)
        if task and task.get("action"):
            actions.add(str(task["action"]))
        scanned += 1
        if scanned >= limit:
            break
    return sorted(actions)


@app.get("/api/queue/{name}")
def api_queue(name: str, _auth: None = Depends(require_auth)):
    stats = admin.queue_stats(name)
    return {"name": name, "stats": stats, "workers": admin.list_workers(name)}


@app.get("/api/queue/{name}/tasks")
def api_queue_tasks(
    name: str,
    state: QueueState = Query(QueueState.all, description="Task state"),
    search: Optional[str] = Query(None, description="Search in task data"),
    action: Optional[str] = Query(None, description="Exact action filter"),
    limit: int = Query(50, ge=1, le=500),
    cursor: Optional[str] = Query(None, description="Search cursor"),
    created_after: Optional[float] = Query(None, description="Unix timestamp"),
    created_before: Optional[float] = Query(None, description="Unix timestamp"),
    completed_after: Optional[float] = Query(None, description="Unix timestamp"),
    completed_before: Optional[float] = Query(None, description="Unix timestamp"),
    _auth: None = Depends(require_auth),
):
    if action or search or state in {QueueState.all, QueueState.history, QueueState.completed,
                 QueueState.failed, QueueState.skipped, QueueState.cancelled,
                 QueueState.retry_wait, QueueState.deadline_missed, QueueState.expired}:
        try:
            page = admin.search_tasks_page(
                [name], state=state, action=action, search=search, limit=limit, cursor=cursor,
                created_after=created_after, created_before=created_before,
                completed_after=completed_after, completed_before=completed_before,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"queue": name, "state": state.value, **page}
    tasks = admin.list_tasks(
        name,
        state=state,
        limit=limit,
        search=search,
        created_after=created_after,
        created_before=created_before,
        completed_after=completed_after,
        completed_before=completed_before,
    )
    return {"queue": name, "state": state.value, "tasks": tasks, "count": len(tasks)}


@app.get("/api/queue/{name}/diagnose")
def api_queue_diagnose(name: str, _auth: None = Depends(require_auth)):
    return admin.diagnose(name)


@app.post("/api/queue/{name}/tasks")
def api_push_task(
    name: str,
    request: PushTaskRequest,
    _auth: None = Depends(require_auth),
):
    if (
        request.duplicate_action == DuplicateAction.ALLOW_NEW
        and not request.confirm_duplicate
    ):
        raise HTTPException(status_code=400, detail="ALLOW_NEW 需要 confirm_duplicate=true")
    try:
        return admin.push_task(
            name,
            request.payload,
            delay_seconds=request.delay_seconds,
            expire_seconds=request.expire_seconds,
            action=request.action,
            logical_key=request.logical_key,
            scheduled_for=request.scheduled_for,
            not_before_at=request.not_before_at,
            start_deadline_at=request.start_deadline_at,
            dedup_until=request.dedup_until,
            trace_id=request.trace_id,
            parent_task_id=request.parent_task_id,
            concurrency_key=request.concurrency_key,
            supersede_key=request.supersede_key,
            supersede_version=request.supersede_version,
            on_duplicate=request.duplicate_action,
        )
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/api/queue/{name}/retry")
def api_retry_queue(name: str, _auth: None = Depends(require_auth)):
    return admin.move_retry(name)


@app.post("/api/queue/{name}/requeue-dlq")
def api_requeue_dlq(
    name: str,
    request: RequeueDlqRequest,
    _auth: None = Depends(require_auth),
):
    if request.task_id is None and not request.confirm_bulk:
        raise HTTPException(status_code=400, detail="批量 replay DLQ 需要 confirm_bulk=true")
    return admin.requeue_dlq(name, task_id=request.task_id)


@app.get("/api/queue/{name}/expired")
def api_queue_expired(
    name: str,
    limit: int = Query(50, ge=1, le=500),
    _auth: None = Depends(require_auth),
):
    tasks = admin.list_expired(name, limit=limit)
    return {"queue": name, "tasks": tasks, "count": len(tasks)}


class RequeueExpiredRequest(BaseModel):
    task_id: Optional[str] = None
    start_deadline_at: datetime


@app.post("/api/queue/{name}/requeue-expired")
def api_requeue_expired(
    name: str,
    request: RequeueExpiredRequest,
    _auth: None = Depends(require_auth),
):
    try:
        return admin.requeue_expired(
            name,
            task_id=request.task_id,
            start_deadline_at=request.start_deadline_at,
        )
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/api/queue/{name}/recover")
def api_recover_queue(
    name: str,
    request: RecoverRequest,
    _auth: None = Depends(require_auth),
):
    if request.include_active and not request.confirm_active:
        raise HTTPException(status_code=400, detail="强制恢复活跃 Worker 需要 confirm_active=true")
    try:
        return admin.recover(name, include_active=request.include_active, worker_id=request.worker_id)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/api/queue/{name}/clear")
def api_clear_queue(
    name: str,
    request: ClearQueueRequest,
    _auth: None = Depends(require_auth),
):
    if (
        request.identity_policy == IdentityPolicy.RELEASE
        and not request.confirm_identity_release
    ):
        raise HTTPException(
            status_code=400,
            detail="释放 logical identity 需要 confirm_identity_release=true",
        )
    if request.include_processing and not request.confirm_processing:
        raise HTTPException(status_code=400, detail="清理 processing 需要 confirm_processing=true")
    try:
        return admin.clear_queue(
            name,
            include_dlq=request.include_dlq,
            include_history=request.include_history,
            identity_policy=request.identity_policy,
            include_processing=request.include_processing,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/tasks")
def api_tasks(
    queue: Optional[str] = Query(None, description="Queue name"),
    status: Optional[str] = Query(None, description="State/status filter"),
    action: Optional[str] = Query(None, description="Filter by action"),
    search: Optional[str] = Query(None, description="Search in task data"),
    limit: int = Query(50, ge=1, le=500),
    cursor: Optional[str] = Query(None, description="Search cursor"),
    created_after: Optional[float] = Query(None, description="Unix timestamp"),
    created_before: Optional[float] = Query(None, description="Unix timestamp"),
    completed_after: Optional[float] = Query(None, description="Unix timestamp"),
    completed_before: Optional[float] = Query(None, description="Unix timestamp"),
    _auth: None = Depends(require_auth),
):
    queues = [queue] if queue else None
    selected_state = QueueState(status) if status in QueueState._value2member_map_ else QueueState.all
    try:
        page = admin.search_tasks_page(
            queues, state=selected_state, action=action, search=search, limit=limit, cursor=cursor,
            created_after=created_after, created_before=created_before,
            completed_after=completed_after, completed_before=completed_before,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {**page, "filters": {"queue": queue, "status": status, "action": action, "search": search}}


@app.get("/api/task/{task_id}")
def api_task(task_id: str, _auth: None = Depends(require_auth)):
    task = admin.get_task(task_id)
    if task:
        return task
    raise HTTPException(status_code=404, detail="Task not found")


@app.get("/api/task/{task_id}/payload")
def api_task_payload(
    task_id: str,
    queue: str = Query(..., description="Queue name"),
    state: str = Query("all", description="Task state"),
    _auth: None = Depends(require_auth),
):
    return admin.resolve_payload(task_id, queue, state)


@app.post("/api/task/{task_id}/requeue")
def api_task_requeue(
    task_id: str,
    request: RequeueTaskRequest,
    _auth: None = Depends(require_auth),
):
    try:
        result = admin.requeue_task(request.queue, task_id, request.from_state)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (TypeError, ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if result["moved"] == 0:
        raise HTTPException(status_code=404, detail="Task not found in requested state")
    return result


@app.post("/api/task/{task_id}/replay")
def api_task_replay(
    task_id: str,
    request: ReplayTaskRequest,
    _auth: None = Depends(require_auth),
):
    try:
        return admin.replay_task(
            task_id,
            queue_name=request.queue,
            start_deadline_at=request.start_deadline_at,
            payload=request.payload,
            logical_key=request.logical_key,
            replace_payload=request.replace_payload,
            replace_logical_key=request.replace_logical_key,
            action=request.action,
        )
    except (KeyError, ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.delete("/api/task/{task_id}")
def api_task_delete(
    task_id: str,
    queue: Optional[str] = Query(None, description="Limit deletion to one queue"),
    _auth: None = Depends(require_auth),
):
    try:
        return admin.delete_task(task_id, queue_name=queue)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.delete("/api/queue/{name}")
def api_delete_queue(
    name: str,
    confirm: bool = Query(False, description="Confirm destructive queue deletion"),
    _auth: None = Depends(require_auth),
):
    if not confirm:
        raise HTTPException(status_code=400, detail="删除队列需要 confirm=true")
    try:
        return admin.delete_queue(name)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    settings = get_auth_settings()
    if settings.enabled and not is_request_authenticated(request):
        return RedirectResponse("/login")
    return FileResponse(SPA_INDEX)


@app.get("/{full_path:path}", include_in_schema=False)
def spa_fallback(full_path: str, request: Request):
    """SPA 客户端路由回退：非 API 路径返回静态文件或 index.html。"""
    if full_path.startswith(("api/", "static/", "assets/")):
        raise HTTPException(status_code=404, detail="Not Found")
    # 认证开启时，SPA 壳页面与静态文件同样要求登录，避免未登录直接查看界面
    settings = get_auth_settings()
    if settings.enabled and not is_request_authenticated(request):
        return RedirectResponse("/login")
    candidate = os.path.realpath(os.path.join(SPA_DIR, full_path))
    if candidate.startswith(os.path.realpath(SPA_DIR) + os.sep) and os.path.isfile(candidate):
        return FileResponse(candidate)
    return FileResponse(SPA_INDEX)


@app.post("/api/queue/{name}/clean-history")
def api_clean_history(
    name: str,
    ttl_days: int = Query(15, ge=0, description="保留最近 N 天历史"),
    _auth: None = Depends(require_auth),
):
    return admin.clean_history(name, ttl_days=ttl_days)


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8765))
    uvicorn.run(app, host="0.0.0.0", port=port)
