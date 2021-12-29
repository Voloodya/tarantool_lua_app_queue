-- Очередь задач с переходом по состояниям

-- Схема

-- Подключение модулей
local uuid = require('uuid')
local log = require('log')
local fiber = require('fiber')

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

local STATUS = {}
STATUS.READY = 'R'
STATUS.TAKEN = 'T'

-- Добавление канала
queue._wait = fiber.channel()

-- Таблицы для хранения “взятых” задач
queue.taken = {} -- список взятых задач
queue.bysid = {} -- список задач для конкретной сессии


-- Автоматический возврат задач в очередь на обрыве соединения ------------
-- Тригер on_connect
box.session.on_connect(function()
    -- Вывод сообщения о подключении
    log.info( "connected %s from %s", box.session.id(), box.session.peer() )
    -- Сохранение пира
    box.session.storage.peer = box.session.peer()
    -- Создание таблицы для хранения задач текущего подключения
    queue.bysid[box.session.id()] = {}
end)

box.session.on_auth(function(user, success)
    if success then
        log.info( "auth %s:%s from %s", box.session.id(), user, box.session.peer() )
    else
        log.warn( "auth %s failed from %s", user, box.session.storage.peer )
    end
end)

-- Тригер on_disconnect
box.session.on_disconnect(function()
    -- Логирование отсоединения, и откуда отсоединился (пиир)
    log.info(
        "disconnected %s:%s from %s", box.session.id(),
        box.session.user(), box.session.storage.peer
    )

    -- Указание, что сессия удалилась
    box.session.storage.destroyed = true

    local sid = box.session.id()
    -- Взятие задачь по данной сессии
    local bysid = queue.bysid[ sid ]
    -- Удаление задачь из списка взятых задач taken и из задач сессии bysid
    while bysid and next(bysid) do
        for key in pairs(bysid) do
            log.info("Autorelease %s by disconnect", key);
            queue.taken[key] = nil
            bysid[key] = nil
            -- Получение задачи (таски) из очереди
            local t = box.space.queue:get(key)
            if t then
                -- Обновление таски
                queue._wait:put(true, 0)
                box.space.queue:update({t.id},{{'=', 'status', STATUS.READY }})
            end
        end
    end
    -- Удаление всех tasks, которые были за данной очередью (сесию)
    queue.bysid[ sid ] = nil
end)
-----------------------------------------------------------------------------------------

-- Возврат при старте (после определения space/index). Обход всех тасков
while true do
    local t = box.space.queue.index.status
        :pairs({STATUS.TAKEN})
        :grep(function(t) return not queue.taken[t.id] end)
        :nth(1)
    if not t then
        break
    end
    box.space.queue:update({t.id}, {{'=', 'status', STATUS.READY}})
    log.info("Autoreleased %s at start", t.id)
end
---------------------------------------------------------------------------------

-- Вставка в таблицу
-- Реализуем put и сгенерируем id для задачи
function queue.put(...)
    local id = uuid():str()
    queue._wait:put(true,0)
    return box.space.queue:insert{ id, STATUS.READY, ... }
end

function queue.take(...)
    timeout = timeout or 0
    -- Когда должно завершиться
    local deadline = fiber.time()+timeout
    local task
    repeat
        -- сработал триггер отключения сессии - выходим
        if box.session.storage.destroyed then return end
        -- Пытаемся забрать task
        task = box.space.queue.index.status:pairs({STATUS.READY}):nth(1)
        if task then break end
        -- Пытаемся взять из канала
        queue._wait:get(deadline-fiber.time())
    until fiber.time() >= deadline

    if not task then return end

    local sid = box.session.id()
    log.info("Register %s by %s", task.id, sid)

    queue.taken[ task.id ] = sid
    queue.bysid[ sid ][ task.id ] = true

    if task then
        return box.space.queue:update({task.id}, {{'=', 'status', STATUS.TAKEN}})
    end

end

-- Общая функцию получения задачи по id с проверкой владения для функций release и ack
local function get_task(key)
    if not key then error("Task id required", 2) end
    -- Получаем таску
    local t = box.space.queue:get{key}
    if not t then
        error(string.format( "Task {%s} was not found", key ), 2)
    end
    -- Взял ли кто-то таску
    if not queue.taken[key] then
        error(string.format( "Task %s not taken by anybody", key ), 2)
    end
    -- Таска взята, но не мы
    if queue.taken[key] ~= box.session.id() then
        error(string.format( "Task %s taken by %d. Not you (%d)",
            key, queue.taken[key], box.session.id() ), 2)
    end
    -- Выход с ошибкой
    if t.status ~= STATUS.TAKEN then
        error("Task not taken")
    end
    -- Иначе возвпащаем таску
    return t
end

-- Функция возврата задач c удалением
function queue.ack(id)
    local t = get_task(id)
    queue.taken[ t.id ] = nil
    queue.bysid[ box.session.id() ][ t.id ] = nil
    return box.space.queue:delete{t.id}
end

-- Возврат задачи в очередь
function queue.release(id)
    local t = get_task(id)
    queue._wait:put(true, 0)
    queue.taken[ t.id ] = nil
    queue.bysid[ box.session.id() ][ t.id ] = nil
    return box.space.queue:update({t.id},{{'=', 'status', STATUS.READY }})
end

return queue