-- Очередь задач с переходом по состояниям

-- Схема

--
local uuid = require('uuid')


box.schema.create_space('queue',{ if_not_exists = true; })

box.space.queue:format( {
  { name = 'id';     type = 'string' },
  { name = 'status'; type = 'string' },
  { name = 'data';   type = '*'      },
})

-- Добавляем индекс по статусу и id (после primary)
box.space.queue:create_index('primary', {
  parts = { 'id' };
  if_not_exists = true;
})

-- Таблица
local queue = {}

-- Рабочие функции ---------
----------------------------

local STATUS = {}
STATUS.READY = 'R'
STATUS.TAKEN = 'T'

-- Вставка в таблицу
-- Реализуем put и сгенерируем id для задачи
function queue.put(...)
    local id = uuid():str()
    return box.space.queue:insert{ id, STATUS.READY, { ... } }
end

function queue.take(...)
    local t = box.space.queue.index.status:pairs({STATUS.READY}):nth(1)
    if t then
        return box.space.queue:update({ t.id }, {{ '=', 'status', STATUS.TAKEN }})
    end
    return
end

-- Функция возврата задач c удалением
function queue.ack(id)
    local t = assert(box.space.queue:get{id},"Task not exists")
    if t.status ~= STATUS.TAKEN then
        error("Task not taken")
    end
    return box.space.queue:delete{t.id}
end

-- Возврат задачи в очередь
function queue.release(id)
    local t = assert(box.space.queue:get{id},"Task not exists")
    if t.status ~= STATUS.TAKEN then
        error("Task not taken")
    end
    return box.space.queue:update({t.id},{{'=', 'status', STATUS.READY }})
end

return queue