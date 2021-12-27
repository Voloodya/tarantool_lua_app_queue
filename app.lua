-- Создана как символическая ссылка на app.lua (ln опции файл_источник файл_ссылки): ln -s app.lua queue1.lua

-- Подключение строгой конфигурации
require('strict').on()

local log = require('log')

-- Подключение библиотеки для определения, как был осуществлен запуск инстанса
fiber = require('fiber');
local under_tarantoolctl = fiber.name() == 'tarantoolctl'

local fio = require('fio');
-- Определение является симлинком файл или нет
local source = fio.abspath(debug.getinfo(1,"S").source:match('^@(.+)'))

--log.warn(debug.getinfo(1, "S"))
--log.warn(debug.getinfo(1, "S").source)

local symlink = fio.readlink(source);
if not symlink then error("Please run by symlink",0) end
-- Получение имени инстанса
local instance_name = fio.basename(source):gsub('%.lua$','')

local data_dir = 'data/'..instance_name

-- Конфигурирование инстанса
local config = {
    pid_file   = data_dir..".pid",
    wal_dir    = data_dir,
    memtx_dir  = data_dir,
    vinyl_dir  = data_dir,
    -- log        = data_dir..".log",
}

do
    local yaml = require('yaml')
    local config_file = os.getenv('CONFIG') or 'etc/'..instance_name..'.yaml'
    local f, e = fio.open(config_file)
    if not f then
        error("Failed to open "..config_file..": "..e, 0)
    end
    local data = f:read()
    local ok, cfg_data = pcall(yaml.decode, data)
    if not ok then
        error("Failed to read "..config_file..": "..cfg_data, 0)
    end
    for k, v in pairs(cfg_data or {}) do
        config[k] = v
    end
end

box.cfg(config)

-- Подключение собственного модуля
queue = require('queue')

if not under_tarantoolctl then
    -- Запуск консоли
    require('console').start()
    os.exit()
end
