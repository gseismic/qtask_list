"""Redis 原子状态转换脚本。

所有脚本都遵守同一约束：先验证消息所有权，再执行状态写入；若消息没有移动，
历史、identity 和指标均不得改变。脚本只面向 standalone/Sentinel Redis 6+。
"""

from __future__ import annotations


ENQUEUE_V2_LUA = r"""
local function key_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then
        return value['ok']
    end
    return value
end

local function redis_now()
    local value = redis.call('TIME')
    return tonumber(value[1]) + tonumber(value[2]) / 1000000
end

local function owner_info(key)
    local kind = key_type(key)
    if kind == 'hash' then
        return redis.call('HGET', key, 'task_id') or '',
               redis.call('HGET', key, 'outcome') or 'none',
               tonumber(redis.call('HGET', key, 'generation') or '0')
    elseif kind == 'string' then
        return redis.call('GET', key) or '', 'none', 0
    end
    return '', '', 0
end

local function supersede_is_newer(candidate, current)
    if not current or current == '' then
        return true
    end
    local candidate_kind = string.sub(candidate, 1, 2)
    local current_kind = string.sub(current, 1, 2)
    if candidate_kind == 'i:' and current_kind == 'i:' then
        return integer_is_newer(string.sub(candidate, 3), string.sub(current, 3))
    end
    if candidate_kind == current_kind then
        return string.sub(candidate, 3) > string.sub(current, 3)
    end
    return candidate > current
end

local now = redis_now()
local has_identity = ARGV[1] == '1'
local duplicate_action = ARGV[2]
local task_id = ARGV[3]
local logical_key = ARGV[4]
local dedup_until = ARGV[5]
local raw_message = ARGV[6]
local available_at = tonumber(ARGV[7])
local record = cjson.decode(ARGV[8])
local queue_name = ARGV[9]
local has_supersede = ARGV[10] == '1'
local supersede_value = ARGV[11]
local created_at = tonumber(ARGV[12])
local fresh_queue = redis.call('ZSCORE', KEYS[7], queue_name) == false

if redis.call('EXISTS', KEYS[5]) == 1 then
    return {'task_id_collision', task_id}
end
if record['payload_pending'] == '1' then
    local pending_key = KEYS[11] .. ':' .. tostring(record['payload_storage_id'])
    local claim_key = 'qtask:storage:claim:' .. tostring(record['payload_storage_id']) .. ':' .. tostring(record['payload_ref'])
    if not redis.call('ZSCORE', pending_key, tostring(record['payload_ref'])) or
       redis.call('EXISTS', claim_key) == 1 then
        return {'payload_expired', task_id}
    end
end

local duplicate_of = ''
local duplicate_outcome = ''
local generation = 1
if has_identity then
    local owner_id, owner_outcome, owner_generation = owner_info(KEYS[3])
    if owner_id == '' then
        if key_type(KEYS[4]) == 'string' then
            owner_id, owner_outcome, owner_generation = owner_info(KEYS[4])
        end
    end
    duplicate_of = owner_id
    duplicate_outcome = owner_outcome
    generation = math.max(owner_generation + 1, 1)

    if duplicate_of ~= '' and duplicate_action ~= 'allow_new' then
        local reason = 'duplicate_active'
        if duplicate_outcome ~= '' and duplicate_outcome ~= 'none' then
            reason = 'duplicate_retained'
        end
        redis.call('HINCRBY', KEYS[9], 'enqueue.' .. reason, 1)
        return {reason, duplicate_of, tostring(owner_generation)}
    end

    -- 旧版 raw logical key 使用 String；ALLOW_NEW 时由新版哈希 owner 接管。
    if key_type(KEYS[3]) ~= 'none' then
        redis.call('DEL', KEYS[3])
    end
    if KEYS[4] ~= KEYS[3] and key_type(KEYS[4]) == 'string' then
        redis.call('DEL', KEYS[4])
    end
    redis.call(
        'HSET', KEYS[3],
        'task_id', task_id,
        'logical_key', logical_key,
        'generation', tostring(generation),
        'outcome', 'none',
        'dedup_until', dedup_until
    )
    redis.call('PERSIST', KEYS[3])
end

for field, value in pairs(record) do
    redis.call('HSET', KEYS[5], field, tostring(value))
end
redis.call('HSET', KEYS[5], 'generation', tostring(generation))
if duplicate_of ~= '' then
    redis.call('HSET', KEYS[5], 'duplicate_override_of', duplicate_of)
end
redis.call('ZADD', KEYS[6], created_at, task_id)
redis.call('ZADD', KEYS[7], now, queue_name)
if fresh_queue and redis.call('ZCARD', KEYS[6]) == 1 then
    redis.call('HSET', KEYS[9], 'observation.version', 2)
end
local start_deadline = tonumber(record['start_deadline_at'] or '')
if start_deadline then
    redis.call('ZADD', KEYS[10], start_deadline, task_id)
end

if has_supersede then
    local current = redis.call('GET', KEYS[8])
    if supersede_is_newer(supersede_value, current) then
        redis.call('SET', KEYS[8], supersede_value)
    end
end

local location = 'ready'
if record['not_before_at'] ~= '' and available_at > now then
    redis.call('ZADD', KEYS[2], available_at, raw_message)
    location = 'delay'
else
    redis.call('LPUSH', KEYS[1], raw_message)
end
redis.call('HSET', KEYS[5], 'location', location)
if record['payload_pending'] == '1' then
    redis.call('ZREM', KEYS[11] .. ':' .. tostring(record['payload_storage_id']), tostring(record['payload_ref']))
end
redis.call('HINCRBY', KEYS[9], 'enqueue.accepted', 1)
if duplicate_of ~= '' then
    redis.call('HINCRBY', KEYS[9], 'enqueue.override', 1)
end
redis.call('HSET', KEYS[5], 'enqueue_confirmed', '1')
return {'enqueued', task_id, duplicate_of, tostring(generation), location, tostring(now)}
"""


BEGIN_ATTEMPT_LUA = r"""
local function key_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then
        return value['ok']
    end
    return value
end

local function redis_now()
    local value = redis.call('TIME')
    return tonumber(value[1]) + tonumber(value[2]) / 1000000
end

local function release_lease()
    if ARGV[6] == '1' and redis.call('GET', KEYS[6]) == ARGV[7] then
        redis.call('DEL', KEYS[6])
    end
end

local function finalize_owner(outcome, now)
    if ARGV[3] ~= '1' or key_type(KEYS[4]) ~= 'hash' then
        return
    end
    if redis.call('HGET', KEYS[4], 'task_id') ~= ARGV[2] then
        return
    end
    redis.call('HSET', KEYS[4], 'outcome', outcome)
    local retain_until = tonumber(redis.call('HGET', KEYS[4], 'dedup_until') or '')
    if retain_until and retain_until > now then
        redis.call('EXPIREAT', KEYS[4], math.ceil(retain_until))
    else
        redis.call('DEL', KEYS[4])
    end
end

local function terminalize(outcome, code, reason, now, operational)
    redis.call(
        'HSET', KEYS[2],
        'outcome', outcome,
        'status', outcome,
        'reason_code', code,
        'reason', reason,
        'finished_at', tostring(now),
        'updated_at', tostring(now),
        'operational_message', operational
    )
    redis.call('HSET', KEYS[2], 'location', operational == '1' and 'dlq' or 'history')
    finalize_owner(outcome, now)
    redis.call('ZREM', KEYS[10], ARGV[2])
    if operational == '0' then
        redis.call('EXPIRE', KEYS[2], tonumber(ARGV[10]))
        if redis.call('HGET', KEYS[2], 'payload_dedicated') == '1' then
            local payload_ref = redis.call('HGET', KEYS[2], 'payload_ref') or ''
            local storage_id = redis.call('HGET', KEYS[2], 'payload_storage_id') or ''
            if payload_ref ~= '' and storage_id ~= '' then
                redis.call('ZADD', KEYS[9] .. ':' .. storage_id, now + tonumber(ARGV[10]), payload_ref)
            end
        end
    end
end

local raw_message = ARGV[1]
local task_id = ARGV[2]
if redis.call('EXISTS', KEYS[2]) == 0 then
    return {'missing_task'}
end
if redis.call('HGET', KEYS[2], 'task_id') ~= task_id then
    return {'task_mismatch'}
end
local outcome = redis.call('HGET', KEYS[2], 'outcome') or ''
if outcome ~= '' and outcome ~= 'none' then
    return {'terminal', outcome}
end

local now = redis_now()
local deadline = tonumber(redis.call('HGET', KEYS[2], 'start_deadline_at') or '')
if deadline and now > deadline then
    local removed = redis.call('LREM', KEYS[1], 1, raw_message)
    if removed == 0 then
        return {'not_found'}
    end
    terminalize('skipped', 'deadline', 'start deadline missed', now, '0')
    release_lease()
    redis.call('HINCRBY', KEYS[8], 'outcome.skipped', 1)
    redis.call('HINCRBY', KEYS[8], 'deadline_missed', 1)
    return {'skipped', tostring(now)}
end

if ARGV[4] == '1' then
    local latest = redis.call('GET', KEYS[5])
    if latest and latest ~= ARGV[5] then
        local removed = redis.call('LREM', KEYS[1], 1, raw_message)
        if removed == 0 then
            return {'not_found'}
        end
        terminalize('cancelled', 'superseded', 'a newer supersede version exists', now, '0')
        release_lease()
        redis.call('HINCRBY', KEYS[8], 'outcome.cancelled', 1)
        redis.call('HINCRBY', KEYS[8], 'superseded', 1)
        return {'cancelled', tostring(now)}
    end
end

local attempt = tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0')
local max_attempts = tonumber(redis.call('HGET', KEYS[2], 'max_attempts') or '1')
if attempt >= max_attempts then
    local removed = redis.call('LREM', KEYS[1], 1, raw_message)
    if removed == 0 then
        return {'not_found'}
    end
    redis.call('LPUSH', KEYS[3], raw_message)
    terminalize('failed', 'attempt_budget', 'attempt budget exhausted before start', now, '1')
    release_lease()
    redis.call('HINCRBY', KEYS[8], 'outcome.failed', 1)
    return {'failed_budget', tostring(attempt)}
end

if ARGV[6] == '1' then
    local acquired = redis.call('SET', KEYS[6], ARGV[7], 'NX', 'PX', tonumber(ARGV[8]))
    if not acquired then
        local run_at = now + tonumber(ARGV[9])
        local deferred_message = '{"available_at":' .. tostring(run_at) .. ARGV[12]
        local removed = redis.call('LREM', KEYS[1], 1, raw_message)
        if removed == 0 then
            return {'not_found'}
        end
        redis.call('ZADD', KEYS[7], run_at, deferred_message)
        redis.call(
            'HSET', KEYS[2],
            'available_at', tostring(run_at),
            'delay_reason', 'concurrency',
            'location', 'delay',
            'updated_at', tostring(now)
        )
        redis.call('HINCRBY', KEYS[8], 'concurrency.deferred', 1)
        return {'deferred', deferred_message, tostring(run_at)}
    end
end

attempt = attempt + 1
local started_message = '{"attempt":' .. tostring(attempt) .. ARGV[11]
local removed = redis.call('LREM', KEYS[1], 1, raw_message)
if removed == 0 then
    release_lease()
    return {'not_found'}
end
redis.call('LPUSH', KEYS[1], started_message)
redis.call('ZREM', KEYS[10], task_id)
redis.call(
    'HSET', KEYS[2],
    'attempt', tostring(attempt),
    'last_started_at', tostring(now),
    'updated_at', tostring(now),
    'available_at', tostring(now),
    'delay_reason', '',
    'claim_token', ARGV[13]
)
redis.call('HSET', KEYS[2], 'location', 'processing')
if not redis.call('HGET', KEYS[2], 'started_at') then
    redis.call('HSET', KEYS[2], 'started_at', tostring(now))
end
redis.call('HINCRBY', KEYS[8], 'attempt.started', 1)
redis.call('HINCRBY', KEYS[8], 'attempt.' .. tostring(attempt), 1)
local created_at = tonumber(redis.call('HGET', KEYS[2], 'created_at') or tostring(now))
redis.call('HINCRBYFLOAT', KEYS[8], 'queue_wait_seconds.total', math.max(now - created_at, 0))
redis.call('HINCRBY', KEYS[8], 'queue_wait_seconds.count', 1)
return {'started', started_message, tostring(attempt), tostring(now)}
"""


COMPLETE_TASK_LUA = r"""
local function key_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then return value['ok'] end
    return value
end
local function redis_now()
    local value = redis.call('TIME')
    return tonumber(value[1]) + tonumber(value[2]) / 1000000
end
local function finalize_owner(outcome, now)
    if ARGV[3] ~= '1' or key_type(KEYS[3]) ~= 'hash' then return end
    if redis.call('HGET', KEYS[3], 'task_id') ~= ARGV[2] then return end
    redis.call('HSET', KEYS[3], 'outcome', outcome)
    local retain_until = tonumber(redis.call('HGET', KEYS[3], 'dedup_until') or '')
    if retain_until and retain_until > now then
        redis.call('EXPIREAT', KEYS[3], math.ceil(retain_until))
    else
        redis.call('DEL', KEYS[3])
    end
end
local raw_message = ARGV[1]
local task_id = ARGV[2]
if redis.call('EXISTS', KEYS[2]) == 0 then return {'missing_task'} end
local outcome = redis.call('HGET', KEYS[2], 'outcome') or ''
if outcome ~= '' and outcome ~= 'none' then return {'terminal', outcome} end
local removed = redis.call('LREM', KEYS[1], 1, raw_message)
if removed == 0 then return {'not_found'} end
local now = redis_now()
redis.call(
    'HSET', KEYS[2],
    'outcome', 'completed',
    'status', 'completed',
    'finished_at', tostring(now),
    'updated_at', tostring(now),
    'operational_message', '0',
    'reason_code', '',
    'reason', ''
)
redis.call('HSET', KEYS[2], 'location', 'history')
if ARGV[6] ~= '' and ARGV[7] == 'full' then
    redis.call('HSET', KEYS[2], 'result', ARGV[6])
end
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[5]))
redis.call('ZREM', KEYS[7], task_id)
if redis.call('HGET', KEYS[2], 'payload_dedicated') == '1' then
    local payload_ref = redis.call('HGET', KEYS[2], 'payload_ref') or ''
    local storage_id = redis.call('HGET', KEYS[2], 'payload_storage_id') or ''
    if payload_ref ~= '' and storage_id ~= '' then
        redis.call('ZADD', KEYS[6] .. ':' .. storage_id, now + tonumber(ARGV[5]), payload_ref)
    end
end
finalize_owner('completed', now)
if ARGV[4] ~= '' and redis.call('GET', KEYS[4]) == ARGV[4] then
    redis.call('DEL', KEYS[4])
end
redis.call('HINCRBY', KEYS[5], 'outcome.completed', 1)
return {'completed', tostring(now)}
"""


RETRY_TASK_LUA = r"""
local function key_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then return value['ok'] end
    return value
end
local function redis_now()
    local value = redis.call('TIME')
    return tonumber(value[1]) + tonumber(value[2]) / 1000000
end
local function finalize_owner(outcome, now)
    if ARGV[8] ~= '1' or key_type(KEYS[4]) ~= 'hash' then return end
    if redis.call('HGET', KEYS[4], 'task_id') ~= ARGV[3] then return end
    redis.call('HSET', KEYS[4], 'outcome', outcome)
    local retain_until = tonumber(redis.call('HGET', KEYS[4], 'dedup_until') or '')
    if retain_until and retain_until > now then
        redis.call('EXPIREAT', KEYS[4], math.ceil(retain_until))
    else
        redis.call('DEL', KEYS[4])
    end
end
local raw_message = ARGV[1]
local next_message = ARGV[2]
local task_id = ARGV[3]
if redis.call('EXISTS', KEYS[2]) == 0 then return {'missing_task'} end
local outcome = redis.call('HGET', KEYS[2], 'outcome') or ''
if outcome ~= '' and outcome ~= 'none' then return {'terminal', outcome} end
local run_at = tonumber(ARGV[4])
local deadline = tonumber(redis.call('HGET', KEYS[2], 'start_deadline_at') or '')
local now = redis_now()
if deadline and run_at >= deadline then
    local removed = redis.call('LREM', KEYS[1], 1, raw_message)
    if removed == 0 then return {'not_found'} end
    redis.call(
        'HSET', KEYS[2],
        'outcome', 'skipped',
        'status', 'skipped',
        'reason_code', 'deadline',
        'reason', 'next retry would miss start deadline',
        'finished_at', tostring(now),
        'updated_at', tostring(now),
        'operational_message', '0'
    )
    redis.call('HSET', KEYS[2], 'location', 'history')
    redis.call('EXPIRE', KEYS[2], tonumber(ARGV[7]))
    redis.call('ZREM', KEYS[8], task_id)
    if redis.call('HGET', KEYS[2], 'payload_dedicated') == '1' then
        local payload_ref = redis.call('HGET', KEYS[2], 'payload_ref') or ''
        local storage_id = redis.call('HGET', KEYS[2], 'payload_storage_id') or ''
        if payload_ref ~= '' and storage_id ~= '' then
            redis.call('ZADD', KEYS[7] .. ':' .. storage_id, now + tonumber(ARGV[7]), payload_ref)
        end
    end
    finalize_owner('skipped', now)
    if ARGV[9] ~= '' and redis.call('GET', KEYS[5]) == ARGV[9] then
        redis.call('DEL', KEYS[5])
    end
    redis.call('HINCRBY', KEYS[6], 'outcome.skipped', 1)
    redis.call('HINCRBY', KEYS[6], 'deadline_missed', 1)
    return {'skipped', tostring(now)}
end
local removed = redis.call('LREM', KEYS[1], 1, raw_message)
if removed == 0 then return {'not_found'} end
redis.call('ZADD', KEYS[3], run_at, next_message)
if deadline then redis.call('ZADD', KEYS[8], deadline, task_id) end
redis.call('HINCRBY', KEYS[6], 'retry_wait.current', 1)
redis.call(
    'HSET', KEYS[2],
    'last_error_code', ARGV[5],
    'last_error', ARGV[6],
    'last_error_at', tostring(now),
    'available_at', tostring(run_at),
    'delay_reason', 'retry',
    'updated_at', tostring(now),
    'operational_message', '1'
)
redis.call('HSET', KEYS[2], 'location', 'delay')
if ARGV[9] ~= '' and redis.call('GET', KEYS[5]) == ARGV[9] then
    redis.call('DEL', KEYS[5])
end
redis.call('HINCRBY', KEYS[6], 'retry.scheduled', 1)
redis.call('HINCRBY', KEYS[6], 'retry.code.' .. ARGV[5], 1)
return {'retry', tostring(run_at)}
"""


FAIL_TASK_LUA = r"""
local function key_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then return value['ok'] end
    return value
end
local function redis_now()
    local value = redis.call('TIME')
    return tonumber(value[1]) + tonumber(value[2]) / 1000000
end
local function finalize_owner(outcome, now)
    if ARGV[6] ~= '1' or key_type(KEYS[4]) ~= 'hash' then return end
    if redis.call('HGET', KEYS[4], 'task_id') ~= ARGV[3] then return end
    redis.call('HSET', KEYS[4], 'outcome', outcome)
    local retain_until = tonumber(redis.call('HGET', KEYS[4], 'dedup_until') or '')
    if retain_until and retain_until > now then
        redis.call('EXPIREAT', KEYS[4], math.ceil(retain_until))
    else
        redis.call('DEL', KEYS[4])
    end
end
local raw_message = ARGV[1]
local dlq_message = ARGV[2]
local task_id = ARGV[3]
if redis.call('EXISTS', KEYS[2]) == 0 then return {'missing_task'} end
local outcome = redis.call('HGET', KEYS[2], 'outcome') or ''
if outcome ~= '' and outcome ~= 'none' then return {'terminal', outcome} end
local removed = redis.call('LREM', KEYS[1], 1, raw_message)
if removed == 0 then return {'not_found'} end
redis.call('LPUSH', KEYS[3], dlq_message)
redis.call('ZREM', KEYS[7], task_id)
local now = redis_now()
redis.call(
    'HSET', KEYS[2],
    'outcome', 'failed',
    'status', 'failed',
    'reason_code', ARGV[4],
    'reason', ARGV[5],
    'last_error_code', ARGV[4],
    'last_error', ARGV[5],
    'last_error_at', tostring(now),
    'finished_at', tostring(now),
    'updated_at', tostring(now),
    'operational_message', '1'
)
redis.call('HSET', KEYS[2], 'location', 'dlq')
finalize_owner('failed', now)
if ARGV[7] ~= '' and redis.call('GET', KEYS[5]) == ARGV[7] then
    redis.call('DEL', KEYS[5])
end
redis.call('HINCRBY', KEYS[6], 'outcome.failed', 1)
redis.call('HINCRBY', KEYS[6], 'failure.code.' .. ARGV[4], 1)
return {'failed', tostring(now)}
"""


CANCEL_OR_PURGE_LUA = r"""
local function key_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then return value['ok'] end
    return value
end
local function redis_now()
    local value = redis.call('TIME')
    return tonumber(value[1]) + tonumber(value[2]) / 1000000
end
local source_type = ARGV[1]
local raw_message = ARGV[2]
local task_id = ARGV[3]
local identity_policy = ARGV[4]
local has_owner = ARGV[5] == '1'
if ARGV[10] ~= '1' and redis.call('EXISTS', KEYS[7]) == 1 then
    return {'active_processing'}
end
local removed = 0
if source_type == 'zset' then
    removed = redis.call('ZREM', KEYS[1], raw_message)
else
    removed = redis.call('LREM', KEYS[1], 1, raw_message)
end
if removed == 0 then return {'not_found'} end

local now = redis_now()
if key_type(KEYS[2]) == 'string' then
    local old_raw = redis.call('GET', KEYS[2])
    local ok, old_record = pcall(cjson.decode, old_raw)
    redis.call('DEL', KEYS[2])
    if ok and type(old_record) == 'table' then
        for field, value in pairs(old_record) do
            if type(value) == 'string' or type(value) == 'number' or type(value) == 'boolean' then
                redis.call('HSET', KEYS[2], field, tostring(value))
            end
        end
    end
    redis.call('HSET', KEYS[2], 'task_id', task_id)
end
redis.call('ZREM', KEYS[8], task_id)
if source_type == 'zset' then
    local ok, envelope = pcall(cjson.decode, raw_message)
    if ok and type(envelope) == 'table' and envelope['delay_reason'] == 'retry' then
        local current = tonumber(redis.call('HGET', KEYS[5], 'retry_wait.current') or '0')
        if current > 0 then redis.call('HINCRBY', KEYS[5], 'retry_wait.current', -1) end
    end
end
local outcome = redis.call('HGET', KEYS[2], 'outcome') or ''
if outcome == '' then
    local legacy_status = redis.call('HGET', KEYS[2], 'status') or ''
    if legacy_status == 'completed' or legacy_status == 'failed' or
       legacy_status == 'skipped' or legacy_status == 'cancelled' then
        outcome = legacy_status
        redis.call('HSET', KEYS[2], 'outcome', outcome)
    end
end
if outcome == '' or outcome == 'none' then
    outcome = 'cancelled'
    redis.call(
        'HSET', KEYS[2],
        'outcome', outcome,
        'status', outcome,
        'reason_code', ARGV[7],
        'reason', ARGV[8],
        'finished_at', tostring(now)
    )
end
redis.call(
    'HSET', KEYS[2],
    'operational_message', '0',
    'updated_at', tostring(now),
    'purged_at', tostring(now)
)
redis.call('HSET', KEYS[2], 'location', 'history')
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[6]))
if redis.call('HGET', KEYS[2], 'payload_dedicated') == '1' then
    local payload_ref = redis.call('HGET', KEYS[2], 'payload_ref') or ''
    local storage_id = redis.call('HGET', KEYS[2], 'payload_storage_id') or ''
    if payload_ref ~= '' and storage_id ~= '' then
        redis.call('ZADD', KEYS[6] .. ':' .. storage_id, now + tonumber(ARGV[6]), payload_ref)
    end
end

if has_owner and key_type(KEYS[3]) == 'hash' and redis.call('HGET', KEYS[3], 'task_id') == task_id then
    if identity_policy == 'release' then
        redis.call('DEL', KEYS[3])
    else
        redis.call('HSET', KEYS[3], 'outcome', outcome)
        local retain_until = tonumber(redis.call('HGET', KEYS[3], 'dedup_until') or '')
        if retain_until and retain_until > now then
            redis.call('EXPIREAT', KEYS[3], math.ceil(retain_until))
        else
            redis.call('DEL', KEYS[3])
        end
    end
end
if ARGV[9] ~= '' and redis.call('GET', KEYS[4]) == ARGV[9] then
    redis.call('DEL', KEYS[4])
end
redis.call('HINCRBY', KEYS[5], 'admin.purged', 1)
if outcome == 'cancelled' then
    redis.call('HINCRBY', KEYS[5], 'outcome.cancelled', 1)
end
return {'removed', outcome, tostring(now)}
"""


ADMIN_MOVE_LUA = r"""
local source_type = ARGV[1]
local raw_message = ARGV[2]
local destination_message = ARGV[3]
local task_id = ARGV[4]
if redis.call('EXISTS', KEYS[3]) == 1 then
    local outcome = redis.call('HGET', KEYS[3], 'outcome') or ''
    if outcome ~= '' and outcome ~= 'none' then
        return {'terminal', outcome}
    end
end
local removed = 0
if source_type == 'zset' then
    removed = redis.call('ZREM', KEYS[1], raw_message)
else
    removed = redis.call('LREM', KEYS[1], 1, raw_message)
end
if removed == 0 then return {'not_found'} end
redis.call('LPUSH', KEYS[2], destination_message)
if redis.call('EXISTS', KEYS[3]) == 1 then
    local value = redis.call('TIME')
    local now = tonumber(value[1]) + tonumber(value[2]) / 1000000
    redis.call(
        'HSET', KEYS[3],
        'operational_message', '1',
        'location', 'ready',
        'manual_moved_at', tostring(now),
        'updated_at', tostring(now)
    )
end
return {'moved', task_id}
"""


MOVE_DUE_DELAY_LUA = r"""
local value = redis.call('TIME')
local now = tonumber(value[1]) + tonumber(value[2]) / 1000000
local count = 0
local limit = tonumber(ARGV[1])
while count < limit do
    local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1)
    if #tasks == 0 then break end
    local task = tasks[1]
    if redis.call('ZREM', KEYS[1], task) == 1 then
        local ok, envelope = pcall(cjson.decode, task)
        if ok and type(envelope) == 'table' and envelope['delay_reason'] == 'retry' then
            local current = tonumber(redis.call('HGET', KEYS[3], 'retry_wait.current') or '0')
            if current > 0 then redis.call('HINCRBY', KEYS[3], 'retry_wait.current', -1) end
        end
        redis.call('LPUSH', KEYS[2], task)
        if ok and type(envelope) == 'table' and envelope['task_id'] then
            local record_key = 'qtask:task:' .. tostring(envelope['task_id'])
            if redis.call('EXISTS', record_key) == 1 then
                redis.call('HSET', record_key, 'location', 'ready')
            end
        end
        count = count + 1
    end
end
return count
"""


RENEW_LEASE_LUA = r"""
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
"""


RETURN_UNSTARTED_CLAIM_LUA = r"""
local raw_message = ARGV[1]
local task_id = ARGV[2]
local expected_attempt = tonumber(ARGV[3])
local lease_token = ARGV[4]
if redis.call('HGET', KEYS[3], 'task_id') ~= task_id then return {'missing_task'} end
local outcome = redis.call('HGET', KEYS[3], 'outcome') or ''
if outcome ~= '' and outcome ~= 'none' then return {'terminal', outcome} end
if tonumber(redis.call('HGET', KEYS[3], 'attempt') or '-1') ~= expected_attempt then
    return {'attempt_changed'}
end
if redis.call('HGET', KEYS[3], 'location') ~= 'processing' then
    return {'location_changed'}
end
if redis.call('LREM', KEYS[1], 1, raw_message) == 0 then return {'not_found'} end
local value = redis.call('TIME')
local now = tonumber(value[1]) + tonumber(value[2]) / 1000000
redis.call('RPUSH', KEYS[2], raw_message)
redis.call(
    'HSET', KEYS[3],
    'attempt', tostring(expected_attempt - 1),
    'location', 'ready',
    'available_at', tostring(now),
    'updated_at', tostring(now),
    'delay_reason', ''
)
local deadline = tonumber(redis.call('HGET', KEYS[3], 'start_deadline_at') or '')
if deadline then redis.call('ZADD', KEYS[4], deadline, task_id) end
if lease_token ~= '' and redis.call('GET', KEYS[5]) == lease_token then
    redis.call('DEL', KEYS[5])
end
redis.call('HINCRBY', KEYS[6], 'attempt.aborted_before_handler', 1)
return {'returned', tostring(now)}
"""


RELEASE_LEASE_LUA = r"""
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call('DEL', KEYS[1])
"""


REPLAY_TASK_LUA = r"""
local function key_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then return value['ok'] end
    return value
end
local function redis_now()
    local value = redis.call('TIME')
    return tonumber(value[1]) + tonumber(value[2]) / 1000000
end
local function owner_info(key)
    local kind = key_type(key)
    if kind == 'hash' then
        return redis.call('HGET', key, 'task_id') or '',
               redis.call('HGET', key, 'outcome') or 'none',
               tonumber(redis.call('HGET', key, 'generation') or '0')
    elseif kind == 'string' then
        return redis.call('GET', key) or '', 'none', 0
    end
    return '', '', 0
end
local function supersede_is_newer(candidate, current)
    if not current or current == '' then return true end
    local candidate_kind = string.sub(candidate, 1, 2)
    local current_kind = string.sub(current, 1, 2)
    if candidate_kind == 'i:' and current_kind == 'i:' then
        return integer_is_newer(string.sub(candidate, 3), string.sub(current, 3))
    end
    if candidate_kind == current_kind then
        return string.sub(candidate, 3) > string.sub(current, 3)
    end
    return candidate > current
end

local source_message = ARGV[1]
local require_source = ARGV[2] == '1'
local old_task_id = ARGV[3]
local new_task_id = ARGV[4]
local has_identity = ARGV[5] == '1'
local logical_key = ARGV[6]
local dedup_until = ARGV[7]
local new_message = ARGV[8]
local available_at = tonumber(ARGV[9])
local record = cjson.decode(ARGV[10])
local queue_name = ARGV[11]
local has_supersede = ARGV[12] == '1'
local supersede_value = ARGV[13]
local created_at = tonumber(ARGV[14])
local ttl_seconds = tonumber(ARGV[15])
local fresh_queue = redis.call('ZSCORE', KEYS[9], queue_name) == false

if redis.call('EXISTS', KEYS[6]) == 0 then return {'missing_source'} end
local old_outcome = redis.call('HGET', KEYS[6], 'outcome') or redis.call('HGET', KEYS[6], 'status') or ''
if old_outcome ~= 'failed' and old_outcome ~= 'skipped' and old_outcome ~= 'cancelled' then
    return {'not_replayable', old_outcome}
end
if redis.call('EXISTS', KEYS[7]) == 1 then return {'task_id_collision'} end
if record['payload_pending'] == '1' then
    local pending_key = KEYS[14] .. ':' .. tostring(record['payload_storage_id'])
    local claim_key = 'qtask:storage:claim:' .. tostring(record['payload_storage_id']) .. ':' .. tostring(record['payload_ref'])
    if not redis.call('ZSCORE', pending_key, tostring(record['payload_ref'])) or
       redis.call('EXISTS', claim_key) == 1 then
        return {'payload_expired', new_task_id}
    end
end

local generation = 1
if has_identity then
    local owner_id, owner_outcome, owner_generation = owner_info(KEYS[4])
    if owner_id == '' then
        if key_type(KEYS[5]) == 'string' then
            owner_id, owner_outcome, owner_generation = owner_info(KEYS[5])
        end
    end
    if owner_id ~= '' and owner_id ~= old_task_id then
        local reason = 'duplicate_active'
        if owner_outcome ~= '' and owner_outcome ~= 'none' then reason = 'duplicate_retained' end
        redis.call('HINCRBY', KEYS[11], 'replay.' .. reason, 1)
        return {reason, owner_id}
    end
    generation = math.max(owner_generation + 1, 1)
end

if require_source then
    local removed = redis.call('LREM', KEYS[1], 1, source_message)
    if removed == 0 then return {'source_not_found'} end
end

local now = redis_now()
redis.call(
    'HSET', KEYS[6],
    'operational_message', '0',
    'location', 'history',
    'replayed_by', new_task_id,
    'updated_at', tostring(now)
)
redis.call('EXPIRE', KEYS[6], ttl_seconds)
if redis.call('HGET', KEYS[6], 'payload_dedicated') == '1' then
    local payload_ref = redis.call('HGET', KEYS[6], 'payload_ref') or ''
    local storage_id = redis.call('HGET', KEYS[6], 'payload_storage_id') or ''
    if payload_ref ~= '' and storage_id ~= '' then
        redis.call('ZADD', KEYS[12] .. ':' .. storage_id, now + ttl_seconds, payload_ref)
    end
end

if has_identity then
    if key_type(KEYS[4]) ~= 'none' then redis.call('DEL', KEYS[4]) end
    if KEYS[5] ~= KEYS[4] and key_type(KEYS[5]) == 'string' then redis.call('DEL', KEYS[5]) end
    redis.call(
        'HSET', KEYS[4],
        'task_id', new_task_id,
        'logical_key', logical_key,
        'generation', tostring(generation),
        'outcome', 'none',
        'dedup_until', dedup_until
    )
    redis.call('PERSIST', KEYS[4])
end

for field, value in pairs(record) do
    redis.call('HSET', KEYS[7], field, tostring(value))
end
redis.call('HSET', KEYS[7], 'generation', tostring(generation))
redis.call('ZADD', KEYS[8], created_at, new_task_id)
redis.call('ZADD', KEYS[9], now, queue_name)
if fresh_queue and redis.call('ZCARD', KEYS[8]) == 1 then
    redis.call('HSET', KEYS[11], 'observation.version', 2)
end
local start_deadline = tonumber(record['start_deadline_at'] or '')
if start_deadline then
    redis.call('ZADD', KEYS[13], start_deadline, new_task_id)
end

if has_supersede then
    local current = redis.call('GET', KEYS[10])
    if supersede_is_newer(supersede_value, current) then
        redis.call('SET', KEYS[10], supersede_value)
    end
end

local location = 'ready'
if record['not_before_at'] ~= '' and available_at > now then
    redis.call('ZADD', KEYS[3], available_at, new_message)
    location = 'delay'
else
    redis.call('LPUSH', KEYS[2], new_message)
end
redis.call('HSET', KEYS[7], 'location', location)
if record['payload_pending'] == '1' then
    redis.call('ZREM', KEYS[14] .. ':' .. tostring(record['payload_storage_id']), tostring(record['payload_ref']))
end
redis.call('HINCRBY', KEYS[11], 'replay.accepted', 1)
redis.call('HINCRBY', KEYS[11], 'enqueue.accepted', 1)
redis.call('HSET', KEYS[7], 'enqueue_confirmed', '1')
return {'enqueued', new_task_id, old_task_id, tostring(generation), location}
"""


# Redis Lua 使用 double；整数版本按十进制文本比较，保持 64 位版本精确。
_INTEGER_COMPARE_LUA = r"""
local function integer_is_newer(candidate, current)
    local candidate_negative = string.sub(candidate, 1, 1) == '-'
    local current_negative = string.sub(current, 1, 1) == '-'
    if candidate_negative ~= current_negative then return not candidate_negative end
    local a = candidate_negative and string.sub(candidate, 2) or candidate
    local b = current_negative and string.sub(current, 2) or current
    if #a ~= #b then
        if candidate_negative then return #a < #b end
        return #a > #b
    end
    if candidate_negative then return a < b end
    return a > b
end
"""

_KEY_TYPES_LUA = r"""
local function assert_key_type(key, allowed)
    local kind = redis.call('TYPE', key)
    if type(kind) == 'table' then kind = kind['ok'] end
    if kind == 'none' then return end
    for expected in string.gmatch(allowed, '[^,]+') do
        if kind == expected then return end
    end
    error('qtask key type conflict: ' .. key .. ' expected ' .. allowed .. ' got ' .. kind)
end
local function assert_counter(key, field, floating)
    local raw = redis.call('HGET', key, field)
    if not raw then return end
    local value = tonumber(raw)
    if not value or value ~= value or value == math.huge or value == -math.huge then
        error('qtask invalid metric counter: ' .. field)
    end
    if not floating then
        if not string.match(raw, '^%-?%d+$') then error('qtask invalid integer counter: ' .. field) end
        local digits = string.sub(raw, 1, 1) == '-' and string.sub(raw, 2) or raw
        if #digits > 19 or (#digits == 19 and digits > '9223372036854775806') then
            error('qtask metric counter overflow: ' .. field)
        end
    end
end
"""

# 仅拒绝实际队列之间的状态路径重叠，独立的 retry/dedup 等业务名称继续可用。
_QUEUE_NAMESPACE_LUA = r"""
local function state_path_overlaps(parent, child)
    for _, segment in ipairs({'processing', 'retry', 'dlq', 'delay', 'worker', 'dedup',
                              'supersede', 'lease', 'maintenance', '__qtask_dummy__'}) do
        local prefix = parent .. ':' .. segment
        if child == prefix or string.sub(child, 1, #prefix + 1) == prefix .. ':' then return true end
    end
    return false
end
local function assert_queue_namespace(candidate)
    for _, existing in ipairs(redis.call('ZRANGE', 'qtask:queues', 0, -1)) do
        if existing ~= candidate and (state_path_overlaps(existing, candidate) or
                                     state_path_overlaps(candidate, existing)) then
            error('qtask queue namespace conflict: ' .. candidate .. ' and ' .. existing)
        end
    end
end
"""

CHECK_QUEUE_NAMESPACE_LUA = (
    _KEY_TYPES_LUA
    + "assert_key_type('qtask:queues', 'zset')\n"
    + _QUEUE_NAMESPACE_LUA
    + "assert_queue_namespace(ARGV[1])\nreturn 1"
)


def _guard_key_types(
    script: str,
    types: dict[int, str],
    *,
    gc: tuple[int, int] | None = None,
    metrics: tuple[int, list[str]] | None = None,
    floating_metrics: tuple[int, list[str]] | None = None,
) -> str:
    """写入前检查键类型，避免 Lua 运行错误留下部分状态。"""
    checks = "\n".join(f"assert_key_type(KEYS[{index}], '{kind}')" for index, kind in types.items())
    if gc is not None:
        prefix, record = gc
        checks += f"""
local storage_id = redis.call('HGET', KEYS[{record}], 'payload_storage_id') or ''
if storage_id ~= '' then assert_key_type(KEYS[{prefix}] .. ':' .. storage_id, 'zset') end
"""
    for floating, counters in ((False, metrics), (True, floating_metrics)):
        if counters is None:
            continue
        key, expressions = counters
        checks += "\n" + "\n".join(
            f"assert_counter(KEYS[{key}], {expression}, {'true' if floating else 'false'})"
            for expression in expressions
        )
    return _KEY_TYPES_LUA + checks + "\n" + script


ENQUEUE_V2_LUA = _guard_key_types(
    _QUEUE_NAMESPACE_LUA
    + "assert_queue_namespace(ARGV[9])\n"
    + _INTEGER_COMPARE_LUA
    + ENQUEUE_V2_LUA,
    {
        1: "list",
        2: "zset",
        3: "hash",
        4: "string,hash",
        5: "hash",
        6: "zset",
        7: "zset",
        8: "string",
        9: "hash",
        10: "zset",
    },
    metrics=(
        9,
        [
            "'enqueue.accepted'",
            "'enqueue.override'",
            "'enqueue.duplicate_active'",
            "'enqueue.duplicate_retained'",
        ],
    ),
)
BEGIN_ATTEMPT_LUA = _guard_key_types(
    BEGIN_ATTEMPT_LUA,
    {
        1: "list",
        2: "hash",
        3: "list",
        4: "hash",
        5: "string",
        6: "string",
        7: "zset",
        8: "hash",
        10: "zset",
    },
    gc=(9, 2),
    metrics=(
        8,
        [
            "'outcome.skipped'",
            "'outcome.cancelled'",
            "'outcome.failed'",
            "'deadline_missed'",
            "'superseded'",
            "'concurrency.deferred'",
            "'attempt.started'",
            "'attempt.' .. tostring(tonumber(redis.call('HGET', KEYS[2], 'attempt') or '0') + 1)",
            "'queue_wait_seconds.count'",
        ],
    ),
    floating_metrics=(8, ["'queue_wait_seconds.total'"]),
)
COMPLETE_TASK_LUA = _guard_key_types(
    COMPLETE_TASK_LUA,
    {
        1: "list",
        2: "hash",
        3: "hash",
        4: "string",
        5: "hash",
        7: "zset",
    },
    gc=(6, 2),
    metrics=(5, ["'outcome.completed'"]),
)
RETRY_TASK_LUA = _guard_key_types(
    RETRY_TASK_LUA,
    {
        1: "list",
        2: "hash",
        3: "zset",
        4: "hash",
        5: "string",
        6: "hash",
        8: "zset",
    },
    gc=(7, 2),
    metrics=(
        6,
        [
            "'outcome.skipped'",
            "'deadline_missed'",
            "'retry_wait.current'",
            "'retry.scheduled'",
            "'retry.code.' .. ARGV[5]",
        ],
    ),
)
FAIL_TASK_LUA = _guard_key_types(
    FAIL_TASK_LUA,
    {
        1: "list",
        2: "hash",
        3: "list",
        4: "hash",
        5: "string",
        6: "hash",
        7: "zset",
    },
    metrics=(6, ["'outcome.failed'", "'failure.code.' .. ARGV[4]"]),
)
REPLAY_TASK_LUA = _guard_key_types(
    _QUEUE_NAMESPACE_LUA
    + "assert_queue_namespace(ARGV[11])\n"
    + _INTEGER_COMPARE_LUA
    + REPLAY_TASK_LUA,
    {
        1: "list",
        2: "list",
        3: "zset",
        4: "hash",
        5: "string,hash",
        6: "hash",
        7: "hash",
        8: "zset",
        9: "zset",
        10: "string",
        11: "hash",
        13: "zset",
    },
    gc=(12, 6),
    metrics=(
        11,
        [
            "'replay.accepted'",
            "'enqueue.accepted'",
            "'replay.duplicate_active'",
            "'replay.duplicate_retained'",
        ],
    ),
)
CANCEL_OR_PURGE_LUA = _guard_key_types(
    CANCEL_OR_PURGE_LUA,
    {
        1: "list,zset",
        2: "hash",
        3: "hash",
        4: "string",
        5: "hash",
        7: "string",
        8: "zset",
    },
    gc=(6, 2),
    metrics=(5, ["'retry_wait.current'", "'admin.purged'", "'outcome.cancelled'"]),
)
