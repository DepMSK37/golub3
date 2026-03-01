/**
 * Голубь Мессенджер — Сервер v5.0
 * + Бот @dirtyexpress
 * + Бот @karnizcal (калькулятор карнизов)
 * + Онлайн/офлайн статус в личных чатах
 * + Переименование группы / аватарка группы
 * + Администраторы группы
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const bcrypt = require('bcryptjs');
const { v4: uuid } = require('uuid');
const Database = require('better-sqlite3');

const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'golub.db');
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    username TEXT UNIQUE NOT NULL COLLATE NOCASE,
    password TEXT NOT NULL,
    avatar_color TEXT DEFAULT '#6C3AE8',
    avatar_data TEXT,
    is_bot INTEGER DEFAULT 0,
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000),
    last_seen INTEGER DEFAULT (strftime('%s','now') * 1000)
  );
  CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000),
    FOREIGN KEY(user_id) REFERENCES users(id)
  );
  CREATE TABLE IF NOT EXISTS chats (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    name TEXT,
    avatar_data TEXT,
    created_by TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000)
  );
  CREATE TABLE IF NOT EXISTS chat_members (
    chat_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    is_admin INTEGER DEFAULT 0,
    PRIMARY KEY(chat_id, user_id)
  );
  CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    chat_id TEXT NOT NULL,
    from_id TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'text',
    text TEXT,
    media_data TEXT,
    media_name TEXT,
    media_size INTEGER,
    media_duration INTEGER,
    reply_to TEXT,
    edited INTEGER DEFAULT 0,
    deleted INTEGER DEFAULT 0,
    created_at INTEGER DEFAULT (strftime('%s','now') * 1000)
  );
  CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(chat_id, created_at);
  CREATE INDEX IF NOT EXISTS idx_cm_user ON chat_members(user_id);
`);

// Migrations
try { db.exec(`ALTER TABLE users ADD COLUMN avatar_data TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE users ADD COLUMN is_bot INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE chat_members ADD COLUMN is_admin INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE chats ADD COLUMN avatar_data TEXT`); } catch(e) {}

// ─── BOT: @ditryexpress (Delivery Bot) — ПОЛНАЯ ВЕРСИЯ как в оригинале ──────
const BOT1_ID = 'bot-dirtyexpress-0000-0000-000000000000';
const BOT1_USERNAME = 'ditryexpress';
const BOT1_NAME = '🚚 DitryExpress';
const BOT1_COLOR = '#FF6B2B';

// ─── BOT: @karnizcal ────────────────────────────────────────────────────────
const BOT2_ID = 'bot-karnizcal-00000-0000-000000000000';
const BOT2_USERNAME = 'karnizcal';
const BOT2_NAME = 'КарнизКал 📐';
const BOT2_COLOR = '#3A8EE8';

function ensureBot(id, username, name, color) {
  const existing = db.prepare('SELECT id FROM users WHERE id=?').get(id);
  if (!existing) {
    db.prepare('INSERT OR IGNORE INTO users(id,name,username,password,avatar_color,is_bot) VALUES(?,?,?,?,?,1)')
      .run(id, name, username, bcrypt.hashSync(uuid(), 10), color);
  }
}
ensureBot(BOT1_ID, BOT1_USERNAME, BOT1_NAME, BOT1_COLOR);
ensureBot(BOT2_ID, BOT2_USERNAME, BOT2_NAME, BOT2_COLOR);

// ─── DELIVERY BOT (@ditryexpress) — ПОЛНАЯ ВЕРСИЯ идентично оригиналу ────────

const ADMIN_PIN = '113337'; // как в оригинале

// Статусы заявок (как в оригинале)
const STATUSES = {
  SENT:       '📨 Ожидает',
  ACCEPTED:   '✅ Принята',
  QUESTION:   '❓ Уточнение',
  SHORTAGE:   '⚠️ Недовоз',
  ASSEMBLING: '🔵 На сборке',
  DELAYED:    '🟡 Задерживается',
  SHIPPED:    '🚚 Отправлено доставкой',
  ON_ROUTE:   '🚗 Выезжаю на доставку',
  CLOSED:     '✅ Закрыта',
  REJECTED:   '❌ Отказ',
};

// ─── Таблицы SQLite для бота ─────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS dlv_users (
    user_id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    name TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS dlv_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL DEFAULT 'SENT',
    created_by TEXT NOT NULL,
    created_by_name TEXT NOT NULL,
    supplier_id TEXT,
    supplier_name TEXT,
    address TEXT NOT NULL,
    contact TEXT NOT NULL,
    items TEXT NOT NULL,
    comment TEXT,
    undelivered TEXT,
    supplier_note TEXT,
    car_info TEXT,
    eta TEXT,
    status_by_name TEXT,
    created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS dlv_additions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    req_id INTEGER NOT NULL,
    created_by TEXT NOT NULL,
    supplier_id TEXT,
    text TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'SENT',
    supplier_note TEXT
  );
  CREATE TABLE IF NOT EXISTS dlv_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS dlv_chat_ids (
    user_id TEXT PRIMARY KEY,
    chat_id TEXT NOT NULL
  );
`);

// ─── DB helpers для бота ──────────────────────────────────────────────────────
const dlvQ = {
  getUser:    db.prepare('SELECT * FROM dlv_users WHERE user_id=?'),
  upsertUser: db.prepare('INSERT OR REPLACE INTO dlv_users(user_id,role,name) VALUES(?,?,?)'),
  allUsers:   db.prepare('SELECT * FROM dlv_users'),
  getReq:     db.prepare('SELECT * FROM dlv_requests WHERE id=?'),
  insertReq:  db.prepare(`INSERT INTO dlv_requests(status,created_by,created_by_name,supplier_id,supplier_name,address,contact,items,comment,undelivered,supplier_note,car_info,eta,status_by_name,created_at) VALUES(@status,@created_by,@created_by_name,@supplier_id,@supplier_name,@address,@contact,@items,@comment,@undelivered,@supplier_note,@car_info,@eta,@status_by_name,@created_at)`),
  updateReq:  db.prepare(`UPDATE dlv_requests SET status=@status,supplier_id=@supplier_id,supplier_name=@supplier_name,supplier_note=@supplier_note,car_info=@car_info,eta=@eta,status_by_name=@status_by_name,undelivered=@undelivered WHERE id=@id`),
  setReqField: db.prepare('UPDATE dlv_requests SET status=? WHERE id=?'),
  allReqs:    db.prepare('SELECT * FROM dlv_requests ORDER BY id DESC'),
  reqsByUser: db.prepare('SELECT * FROM dlv_requests WHERE created_by=? ORDER BY id DESC'),
  reqsForSup: db.prepare("SELECT * FROM dlv_requests WHERE (supplier_id=? OR (supplier_id IS NULL AND status='SENT')) AND status NOT IN ('CLOSED','REJECTED') ORDER BY id DESC"),
  openForSup: db.prepare("SELECT * FROM dlv_requests WHERE (supplier_id=? OR (supplier_id IS NULL AND status='SENT')) AND status NOT IN ('CLOSED','REJECTED') ORDER BY id DESC LIMIT 20"),
  getAdd:     db.prepare('SELECT * FROM dlv_additions WHERE id=?'),
  insertAdd:  db.prepare('INSERT INTO dlv_additions(req_id,created_by,supplier_id,text,status,supplier_note) VALUES(?,?,?,?,?,?)'),
  updateAdd:  db.prepare('UPDATE dlv_additions SET status=?,supplier_note=? WHERE id=?'),
  addsByReq:  db.prepare('SELECT * FROM dlv_additions WHERE req_id=? ORDER BY id DESC LIMIT 10'),
  qAddsByReq: db.prepare("SELECT * FROM dlv_additions WHERE req_id=? AND status='QUESTION' ORDER BY id DESC"),
  getSetting: db.prepare('SELECT value FROM dlv_settings WHERE key=?'),
  setSetting: db.prepare('INSERT OR REPLACE INTO dlv_settings(key,value) VALUES(?,?)'),
  getChatId:  db.prepare('SELECT chat_id FROM dlv_chat_ids WHERE user_id=?'),
  setChatId:  db.prepare('INSERT OR REPLACE INTO dlv_chat_ids(user_id,chat_id) VALUES(?,?)'),
};

function dlvGetUser(uid)      { return dlvQ.getUser.get(uid) || null; }
function dlvSetUser(uid, role, name) { dlvQ.upsertUser.run(uid, role, name); }
function dlvAllUsers()        { return dlvQ.allUsers.all(); }
function dlvSuppliers()       { return dlvQ.allUsers.all().filter(u => u.role === 'SUPPLIER'); }
function dlvGetReq(id)        { return dlvQ.getReq.get(id) || null; }
function dlvCreateReq(data)   { return dlvQ.insertReq.run(data).lastInsertRowid; }
function dlvUpdateReq(r)      { dlvQ.updateReq.run(r); }
function dlvCloseReq(id, st)  { dlvQ.setReqField.run(st, id); }
function dlvAllReqs()         { return dlvQ.allReqs.all(); }
function dlvReqsByUser(uid)   { return dlvQ.reqsByUser.all(uid); }
function dlvReqsForSup(uid)   { return dlvQ.reqsForSup.all(uid); }
function dlvOpenForSup(uid)   { return dlvQ.openForSup.all(uid); }
function dlvGetAdd(id)        { return dlvQ.getAdd.get(id) || null; }
function dlvCreateAdd(reqId, createdBy, supId, text) { return dlvQ.insertAdd.run(reqId, createdBy, supId||null, text, 'SENT', null).lastInsertRowid; }
function dlvUpdateAdd(id, st, note) { dlvQ.updateAdd.run(st, note||null, id); }
function dlvAddsByReq(reqId)  { return dlvQ.addsByReq.all(reqId); }
function dlvQAddsByReq(reqId) { return dlvQ.qAddsByReq.all(reqId); }
function dlvGetSetting(k,def) { const r=dlvQ.getSetting.get(k); return r?r.value:def; }
function dlvSetSetting(k,v)   { dlvQ.setSetting.run(k,v); }
function dlvGetChatId(uid)    { const r=dlvQ.getChatId.get(uid); return r?r.chat_id:null; }
function dlvSetChatId(uid, chatId) { dlvQ.setChatId.run(uid, chatId); }

// ─── Состояние диалога (в памяти) ────────────────────────────────────────────
const b1State = new Map(); // userId => { step, ... }

// Настройки напоминаний
let reminderMode = dlvGetSetting('reminder_mode', 'off');
let reminderTimers = [];

function b1Reply(text, buttons) { return { text, buttons: buttons || null }; }

function shortReqLine(r) {
  return `#${r.id} | ${(r.address||'').substring(0,40)} | ${STATUSES[r.status]||r.status}`;
}

function fmtReq(r) {
  const adds = dlvAddsByReq(r.id);
  const addsBlock = adds.length
    ? adds.slice(-5).map(a => `• Доп. №${a.id} — ${a.status}${a.supplier_note ? ` | ❓ ${a.supplier_note}` : ''}\n${a.text}`).join('\n\n')
    : '—';
  const extra = [];
  if (r.car_info && r.status === 'SHIPPED') extra.push(`Авто: ${r.car_info}`);
  if (r.eta    && r.status === 'ON_ROUTE') extra.push(`Время прибытия: ${r.eta}`);
  if (r.status_by_name) extra.push(`Поставил: ${r.status_by_name}`);
  return (
    `**Заявка #${r.id}**\n` +
    `Статус: **${STATUSES[r.status]||r.status}**${extra.length ? '\n'+extra.join('\n') : ''}\n` +
    `Уточнение: ${r.supplier_note||'—'}\n\n` +
    `📍 Адрес: ${r.address}\n` +
    `👤 Контакт: ${r.contact}\n\n` +
    `📦 Материалы:\n${r.items}\n\n` +
    `➕ **Последние дополнения:**\n${addsBlock}\n\n` +
    `⚠️ **Недовоз:** ${r.undelivered||'—'}\n\n` +
    `💬 Комментарий: ${r.comment||'—'}\n` +
    `🏭 Поставщик: ${r.supplier_name||'не назначен'}`
  );
}

function b1MainMenu(userId) {
  const u = dlvGetUser(userId);
  if (!u) return b1Reply(
    `👋 Привет! Я **DitryExpress** 🚚\nБот управления доставками\n\nВыберите вашу роль:`,
    [{label:'👷 Менеджер', value:'__role_MANAGER'}, {label:'🚚 Поставщик', value:'__role_SUPPLIER'}, {label:'🛂 Admin', value:'__role_ADMIN'}]
  );
  if (u.role === 'MANAGER') return b1Reply(
    `📋 **Меню менеджера**\nДобрый день, **${u.name}**!`,
    [{label:'➕ Создать заявку', value:'__mgr_create'}, {label:'🟦 Активные заявки', value:'__mgr_list'}, {label:'🔄 Сменить роль', value:'__change_role'}]
  );
  if (u.role === 'SUPPLIER') return b1Reply(
    `🚛 **Меню поставщика**\nДобрый день, **${u.name}**!`,
    [{label:'🟦 Активные заявки', value:'__sup_list'}, {label:'🔄 Сменить роль', value:'__change_role'}]
  );
  if (u.role === 'ADMIN') return b1Reply(
    `🛂 **Панель администратора**\nДобрый день, **${u.name}**!`,
    [
      {label:'👁 Просмотр заявок', value:'__adm_view'},
      {label:'✏️ Редактирование заявок', value:'__adm_edit_start'},
      {label:'🔔 Напомнить поставщику', value:'__adm_remind_menu'},
      {label:'👥 Участники', value:'__adm_users'},
      {label:'🔄 Сменить роль', value:'__change_role'}
    ]
  );
  return b1Reply('/start — начать заново');
}

function getSupplierActiveChatId(userId) {
  // Найти chatId бота для конкретного пользователя
  return dlvGetChatId(userId) || null;
}

// Хранит chatId чата с ботом для каждого пользователя (заполняется при создании/открытии чата)

function processBot1(text, userId, chatId) {
  if (chatId) dlvSetChatId(userId, chatId);
  if (!text) return null;
  text = text.trim();
  const state = b1State.get(userId) || { step: 'menu' };

  if (text === '/start' || text === '/menu') {
    b1State.set(userId, { step: 'menu' });
    return b1MainMenu(userId);
  }
  if (text === '/help') {
    return b1Reply(
      `🚚 **DitryExpress** — Помощь\n\n👷 **Менеджер** — создаёт заявки на доставку\n🚛 **Поставщик** — принимает и выполняет заявки\n🛂 **Администратор** — полный доступ\n\n📋 Команды:\n/start — главное меню\n/help — эта справка\n/stop — сбросить сессию`,
      [{label:'🏠 Главное меню', value:'/start'}]
    );
  }
  if (text === '/stop') {
    b1State.delete(userId);
    return b1Reply(`✋ Сессия сброшена. Нажмите /start для начала.`, [{label:'🏠 Начать', value:'/start'}]);
  }

  // Смена роли
  if (text === '__change_role' || text === '🔄 Сменить роль') {
    b1State.set(userId, { step: 'pick_role' });
    return b1Reply(`🔄 Выберите новую роль:`,
      [{label:'👷 Менеджер', value:'__role_MANAGER'}, {label:'🚚 Поставщик', value:'__role_SUPPLIER'}, {label:'🛂 Admin', value:'__role_ADMIN'}]
    );
  }

  // Регистрация роли
  if (text.startsWith('__role_')) {
    const role = text.replace('__role_', '');
    if (role === 'ADMIN') {
      b1State.set(userId, { step: 'admin_pin' });
      return b1Reply(`🛂 Введите PIN-код администратора:`);
    }
    if (role === 'MANAGER' || role === 'SUPPLIER') {
      b1State.set(userId, { step: 'set_name', role });
      return b1Reply(`✅ Роль: ${role === 'MANAGER' ? 'Менеджер' : 'Поставщик'}\n\n📝 Введите ваше имя:`);
    }
  }

  if (state.step === 'set_name') {
    dlvSetUser(userId, state.role, text);
    b1State.set(userId, { step: 'menu' });
    return b1MainMenu(userId);
  }

  if (state.step === 'admin_pin') {
    if (text === ADMIN_PIN) {
      b1State.set(userId, { step: 'set_admin_name' });
      return b1Reply(`✅ Доступ разрешён.\n\n📝 Введите ваше имя:`);
    }
    b1State.set(userId, { step: 'menu' });
    return b1Reply(`❌ Неверный PIN.`, [{label:'🔁 Попробовать снова', value:'__role_ADMIN'}, {label:'🏠 В меню', value:'/start'}]);
  }

  if (state.step === 'set_admin_name') {
    dlvSetUser(userId, 'ADMIN', text);
    b1State.set(userId, { step: 'menu' });
    return b1MainMenu(userId);
  }

  const u = dlvGetUser(userId);
  if (!u) {
    b1State.set(userId, { step: 'menu' });
    return b1MainMenu(userId);
  }

  // ══════════════════════════════════════════════════════════════
  // МЕНЕДЖЕР
  // ══════════════════════════════════════════════════════════════
  if (u.role === 'MANAGER') {

    // Создать заявку — шаг 1: выбор поставщика
    if (text === '__mgr_create') {
      const sups = dlvSuppliers();
      if (sups.length) {
        const btns = sups.map(s => ({label:`🚚 ${s.name}`, value:`__sup_pick_${s.user_id}`}));
        btns.push({label:'🚫 Без поставщика', value:'__sup_pick_none'});
        b1State.set(userId, { step: 'req_supplier', req: {} });
        return b1Reply(`➕ **Новая заявка**\n\nВыберите поставщика:`, btns);
      }
      b1State.set(userId, { step: 'req_address', req: { supplierId: null, supplierName: '—' } });
      return b1Reply(`➕ **Новая заявка**\nПоставщиков нет — создадим без привязки.\n\nШаг 1/4: Введите адрес/объект доставки:`);
    }

    if (state.step === 'req_supplier') {
      if (text === '__sup_pick_none') {
        b1State.set(userId, { step: 'req_address', req: { supplier_id: null, supplier_name: '—' } });
        return b1Reply(`Шаг 1/4: Введите адрес/объект доставки:`);
      }
      if (text.startsWith('__sup_pick_')) {
        const sid = text.replace('__sup_pick_', '');
        const sv = dlvGetUser(sid);
        b1State.set(userId, { step: 'req_address', req: { supplier_id: sid, supplier_name: sv?.name || '—' } });
        return b1Reply(`Шаг 1/4: Введите адрес/объект доставки:`);
      }
      return b1Reply(`Выберите поставщика из кнопок выше.`);
    }

    if (state.step === 'req_address') {
      b1State.set(userId, { ...state, step: 'req_contact', req: { ...state.req, address: text } });
      return b1Reply(`Шаг 2/4: Контактное лицо на объекте:`);
    }

    if (state.step === 'req_contact') {
      b1State.set(userId, { ...state, step: 'req_items', req: { ...state.req, contact: text } });
      return b1Reply(`Шаг 3/4: Материалы списком.\nПример:\n1) Цемент — 20 мешков\n2) Арматура — 150 м\n\nВведите список:`);
    }

    if (state.step === 'req_items') {
      b1State.set(userId, { ...state, step: 'req_comment', req: { ...state.req, items: text } });
      return b1Reply(`Шаг 4/4: Комментарий (если нет — нажмите кнопку):`, [{label:'⏭ Без комментария', value:'__no_comment'}]);
    }

    if (state.step === 'req_comment') {
      const comment = text === '__no_comment' ? '' : text;
      const req = state.req;
      // Показываем превью и просим подтвердить
      const preview = (
        `🧾 **Заявка на доставку материалов**\n\n` +
        `📍 **Адрес/объект:** ${req.address}\n` +
        `👤 **Контакт:** ${req.contact}\n\n` +
        `📦 **Материалы:**\n${req.items}\n\n` +
        `💬 **Комментарий:** ${comment || '—'}\n` +
        `🏭 **Поставщик:** ${req.supplier_name}`
      );
      b1State.set(userId, { step: 'req_confirm', req: { ...req, comment } });
      return b1Reply(preview, [
        {label:'✅ Подтвердить', value:'__confirm_send'},
        {label:'✏️ Исправить заново', value:'__confirm_restart'},
        {label:'❌ Отмена', value:'__confirm_cancel'},
      ]);
    }

    if (state.step === 'req_confirm') {
      if (text === '__confirm_cancel') {
        b1State.set(userId, { step: 'menu' });
        return b1Reply(`Отменено.`, [{label:'🏠 Меню', value:'/start'}]);
      }
      if (text === '__confirm_restart') {
        b1State.set(userId, { step: 'menu' });
        return b1Reply(`Ок, начнём заново.`, [{label:'➕ Создать заявку', value:'__mgr_create'}]);
      }
      if (text === '__confirm_send') {
        const req = state.req;
        const newReq = {
          status: 'SENT', created_by: userId, created_by_name: u.name,
          supplier_id: req.supplier_id||null, supplier_name: req.supplier_name||null,
          address: req.address, contact: req.contact, items: req.items,
          comment: req.comment||null, undelivered: null, supplier_note: null,
          car_info: null, eta: null, status_by_name: null,
          created_at: new Date().toLocaleString('ru-RU')
        };
        const id = dlvCreateReq(newReq);
        b1State.set(userId, { step: 'menu' });
        // Уведомление поставщику
        if (req.supplier_id && req.supplier_id !== 'none') {
          const supChatId = dlvGetChatId(req.supplier_id);
          if (supChatId) {
            setTimeout(() => {
              const memberIds = chatMemberIds(supChatId);
              const supText = (
                `📨 Новая заявка **#${id}**\n\n` +
                `📍 **Адрес:** ${req.address}\n` +
                `👤 **Контакт:** ${req.contact}\n\n` +
                `📦 **Материалы:**\n${req.items}\n\n` +
                `💬 **Комментарий:** ${req.comment||'—'}\n` +
                `Создатель: ${u.name}\n\nВыберите статус:`
              );
              sendBotMsg(supChatId, supText, memberIds, [
                {label:'✅ Принять', value:`__sup_accept_${id}`},
                {label:'❓ Уточнить', value:`__sup_question_${id}`},
                {label:'❌ Отказ', value:`__sup_reject_${id}`},
              ]);
            }, 500);
          }
        }
        return b1Reply(
          `✅ Заявка отправлена. Номер: **#${id}**`,
          [{label:'➕ Ещё заявку', value:'__mgr_create'}, {label:'🟦 Мои заявки', value:'__mgr_list'}, {label:'🏠 Меню', value:'/start'}]
        );
      }
    }

    // Список заявок менеджера
    if (text === '__mgr_list') {
      const myReqs = dlvReqsByUser(userId);
      if (!myReqs.length) return b1Reply(`📭 У вас нет заявок.`, [{label:'➕ Создать заявку', value:'__mgr_create'}, {label:'🏠 Меню', value:'/start'}]);
      const btns = myReqs.slice(-10).map(r => ({label: shortReqLine(r), value:`__view_user_${r.id}`}));
      btns.push({label:'🏠 Меню', value:'/start'});
      return b1Reply(`🟦 **Мои активные заявки:**`, btns);
    }

    // Просмотр заявки менеджером
    if (text.startsWith('__view_user_')) {
      const id = parseInt(text.replace('__view_user_', ''));
      const r = dlvGetReq(id);
      if (!r) return b1Reply(`❌ Заявка не найдена`, [{label:'⬅️ Назад', value:'__mgr_list'}]);
      const btns = [];
      if (r.status === 'QUESTION') btns.push({label:'💬 Ответить поставщику', value:`__user_answer_req_${id}`});
      btns.push({label:'✅ Закрыть заявку', value:`__user_close_${id}`});
      btns.push({label:'⚠️ Указать недовезённое', value:`__user_shortage_${id}`});
      btns.push({label:'➕ Дополнить заказ', value:`__user_additems_${id}`});
      // Ответ на вопрос по дополнению
      const qAdds = dlvQAddsByReq(id);
      if (qAdds.length) btns.push({label:'💬 Ответить на вопрос (доп.)', value:`__user_answer_add_${id}`});
      btns.push({label:'⬅️ Назад к активным', value:'__mgr_list'});
      return b1Reply(fmtReq(r), btns);
    }

    // Закрыть заявку
    if (text.startsWith('__user_close_')) {
      const id = parseInt(text.replace('__user_close_', ''));
      const r = dlvGetReq(id);
      if (r) {
        r.status = 'CLOSED'; dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:r.id});
        if (r.supplier_id) {
          const supChatId = dlvGetChatId(r.supplier_id);
          if (supChatId) setTimeout(() => sendBotMsg(supChatId, `✅ Заявка **#${id}** закрыта менеджером.`, chatMemberIds(supChatId)), 500);
        }
      }
      return b1Reply(`✅ Заявка **#${id}** закрыта.`, [{label:'🟦 Мои заявки', value:'__mgr_list'}, {label:'🏠 Меню', value:'/start'}]);
    }

    // Недовоз — старт
    if (text.startsWith('__user_shortage_')) {
      const id = parseInt(text.replace('__user_shortage_', ''));
      b1State.set(userId, { step: 'shortage_input', reqId: id });
      return b1Reply(`⚠️ Заявка **#${id}**: напишите недовезённые материалы списком:`);
    }
    if (state.step === 'shortage_input') {
      const r = dlvGetReq(state.reqId);
      if (r) { r.undelivered = text; dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:state.reqId});
        if (r.supplier_id) {
          const supChatId = dlvGetChatId(r.supplier_id);
          if (supChatId) setTimeout(() => sendBotMsg(supChatId, `⚠️ Недовоз по заявке **#${state.reqId}**\n\nНедовезено:\n${text}`, chatMemberIds(supChatId)), 500);
        }
      }
      b1State.set(userId, { step: 'menu' });
      return b1Reply(
        `⚠️ Недовоз по заявке **#${state.reqId}** сохранён${r?.supplier_id ? ' и отправлен поставщику' : ''}.`,
        [{label:'🟦 Мои заявки', value:'__mgr_list'}, {label:'🏠 Меню', value:'/start'}]
      );
    }

    // Дополнение — старт
    if (text.startsWith('__user_additems_')) {
      const id = parseInt(text.replace('__user_additems_', ''));
      const r = dlvGetReq(id);
      if (r?.status === 'CLOSED') return b1Reply(`Заявка уже закрыта — дополнение невозможно.`, [{label:'⬅️ Назад', value:`__view_user_${id}`}]);
      b1State.set(userId, { step: 'additems_input', reqId: id });
      return b1Reply(`➕ Заявка **#${id}**: напишите что нужно ДОБАВИТЬ к заказу списком:`);
    }
    if (state.step === 'additems_input') {
      const r = dlvGetReq(state.reqId);
      if (!r) { b1State.set(userId, {step:'menu'}); return b1MainMenu(userId); }
      const addId = dlvCreateAdd(state.reqId, userId, r.supplier_id, text);
      b1State.set(userId, { step: 'menu' });
      // Уведомление поставщику
      if (r.supplier_id) {
        const supChatId = dlvGetChatId(r.supplier_id);
        if (supChatId) {
          setTimeout(() => {
            sendBotMsg(supChatId,
              `➕ **Дополнение №${addId}** к заявке **#${state.reqId}**\n\nДобавить:\n${text}\n\nПодтвердите дополнение:`,
              chatMemberIds(supChatId),
              [
                {label:'✅ Принял дополнение', value:`__add_accept_${addId}`},
                {label:'❓ Ещё вопрос', value:`__add_question_${addId}`},
                {label:'❌ Не могу выполнить', value:`__add_reject_${addId}`},
              ]
            );
          }, 500);
        }
      }
      return b1Reply(
        `➕ Дополнение к заявке **#${state.reqId}** ${r.supplier_id ? 'отправлено поставщику' : 'сохранено'} (доп. №${addId}).`,
        [{label:'🟦 Мои заявки', value:'__mgr_list'}, {label:'🏠 Меню', value:'/start'}]
      );
    }

    // Ответ менеджера на вопрос поставщика по заявке
    if (text.startsWith('__user_answer_req_')) {
      const id = parseInt(text.replace('__user_answer_req_', ''));
      const r = dlvGetReq(id);
      if (!r || r.status !== 'QUESTION') return b1Reply(`По этой заявке нет запроса уточнения.`, [{label:'⬅️ Назад', value:`__view_user_${id}`}]);
      b1State.set(userId, { step: 'answer_req_input', reqId: id });
      return b1Reply(`❓ Вопрос поставщика по заявке **#${id}**:\n${r.supplier_note}\n\nНапишите ответ поставщику:`);
    }
    if (state.step === 'answer_req_input') {
      const r = dlvGetReq(state.reqId);
      if (r && r.supplier_id) {
        const supChatId = dlvGetChatId(r.supplier_id);
        if (supChatId && r.supplier_id !== userId) {
          const answer = text;
          setTimeout(() => {
            sendBotMsg(supChatId,
              `💬 Ответ менеджера по заявке **#${state.reqId}**:\n\n${answer}\n\nПожалуйста, подтвердите заявку:`,
              chatMemberIds(supChatId),
              [
                {label:'✅ Принять', value:`__sup_accept_${state.reqId}`},
                {label:'❓ Уточнить', value:`__sup_question_${state.reqId}`},
                {label:'❌ Отказ', value:`__sup_reject_${state.reqId}`},
              ]
            );
          }, 500);
        }
      }
      b1State.set(userId, { step: 'menu' });
      return b1Reply(`Ответ отправлен поставщику.`, [{label:'🟦 Мои заявки', value:'__mgr_list'}]);
    }

    // Ответ менеджера на вопрос поставщика по дополнению
    if (text.startsWith('__user_answer_add_')) {
      const reqId = parseInt(text.replace('__user_answer_add_', ''));
      const qAdds = dlvQAddsByReq(reqId);
      if (!qAdds.length) return b1Reply(`Нет дополнений с вопросом.`, [{label:'⬅️ Назад', value:`__view_user_${reqId}`}]);
      const btns = qAdds.map(a => ({label:`Доп. №${a.id} — ${(a.supplier_note||'').substring(0,40)}`, value:`__user_answer_add_pick_${a.id}`}));
      btns.push({label:'⬅️ Назад', value:`__view_user_${reqId}`});
      return b1Reply(`Выберите дополнение:`, btns);
    }
    if (text.startsWith('__user_answer_add_pick_')) {
      const addId = parseInt(text.replace('__user_answer_add_pick_', ''));
      const a = dlvGetAdd(addId);
      if (!a) return b1Reply(`Дополнение не найдено.`);
      b1State.set(userId, { step: 'answer_add_input', addId });
      return b1Reply(`❓ Вопрос по доп. №${addId}:\n${a.supplier_note}\n\nНапишите ответ поставщику:`);
    }
    if (state.step === 'answer_add_input') {
      const a = dlvGetAdd(state.addId);
      if (a && a.supplier_id) {
        const supChatId = dlvGetChatId(a.supplier_id);
        if (supChatId && a.supplier_id !== userId) {
          const answer = text;
          setTimeout(() => {
            sendBotMsg(supChatId,
              `💬 Ответ менеджера по доп. **№${state.addId}** (заявка **#${a.req_id}**):\n\n${answer}\n\nПожалуйста, подтвердите дополнение:`,
              chatMemberIds(supChatId),
              [
                {label:'✅ Принял дополнение', value:`__add_accept_${state.addId}`},
                {label:'❓ Ещё вопрос', value:`__add_question_${state.addId}`},
                {label:'❌ Не могу выполнить', value:`__add_reject_${state.addId}`},
              ]
            );
          }, 500);
        }
      }
      b1State.set(userId, { step: 'menu' });
      return b1Reply(`Ответ отправлен поставщику.`, [{label:'🏠 Меню', value:'/start'}]);
    }

    if (state.step === 'menu') return b1MainMenu(userId);
  }

  // ══════════════════════════════════════════════════════════════
  // ПОСТАВЩИК
  // ══════════════════════════════════════════════════════════════
  if (u.role === 'SUPPLIER') {

    // Список активных заявок поставщика
    if (text === '__sup_list') {
      const myReqs = dlvReqsForSup(userId);
      if (!myReqs.length) return b1Reply(`📭 Нет активных заявок.`, [{label:'🏠 Меню', value:'/start'}]);
      const btns = myReqs.slice(-10).map(r => ({label: shortReqLine(r), value:`__view_sup_${r.id}`}));
      btns.push({label:'🏠 Меню', value:'/start'});
      return b1Reply(`🟦 **Активные заявки (поставщик):**`, btns);
    }

    // Просмотр заявки поставщиком
    if (text.startsWith('__view_sup_')) {
      const id = parseInt(text.replace('__view_sup_', ''));
      const r = dlvGetReq(id);
      if (!r) return b1Reply(`❌ Заявка не найдена`, [{label:'⬅️ Назад', value:'__sup_list'}]);
      const mine = r.supplier_id === userId;
      const canTake = !r.supplier_id;
      const btns = [];
      if (!['ACCEPTED','ASSEMBLING','DELAYED','SHIPPED','ON_ROUTE'].includes(r.status) && (canTake || mine)) {
        btns.push({label:'✅ Принять', value:`__sup_accept_${id}`});
      }
      if (mine || canTake) btns.push({label:'📌 Статус', value:`__sup_status_${id}`});
      btns.push({label:'❓ Уточнить', value:`__sup_question_${id}`});
      if (!['REJECTED','CLOSED'].includes(r.status)) btns.push({label:'❌ Отказ', value:`__sup_reject_${id}`});
      btns.push({label:'⬅️ Назад к активным', value:'__sup_list'});
      let header = '';
      if (canTake && !['REJECTED','CLOSED'].includes(r.status)) header = '🟡 **Заявка пока без поставщика.** Нажмите **Принять**, чтобы взять в работу.\n\n';
      else if (r.supplier_id && r.supplier_id !== userId) header = `ℹ️ **Заявка назначена другому поставщику:** ${r.supplier_name}\n\n`;
      return b1Reply(header + fmtReq(r), btns);
    }

    // Принять заявку
    if (text.startsWith('__sup_accept_')) {
      const id = parseInt(text.replace('__sup_accept_', ''));
      const r = dlvGetReq(id);
      if (!r) return b1Reply(`❌ Заявка не найдена`, [{label:'⬅️ Назад', value:'__sup_list'}]);
      if (!r.supplier_id) { r.supplier_id = userId; r.supplier_name = u.name; }
      r.status = 'ACCEPTED';
      dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:r.id});
      dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:r.id});
      // Уведомление менеджеру
      const mgrChatId = dlvGetChatId(r.created_by);
      if (mgrChatId && r.created_by !== userId) {
        setTimeout(() => {
          const memberIds = chatMemberIds(mgrChatId);
          sendBotMsg(mgrChatId, `✅ Заявка **#${id}** принята поставщиком: **${u.name}**.`, memberIds, [{label:'🟦 Мои заявки', value:'__mgr_list'}]);
        }, 500);
      }
      return b1Reply(
        `✅ Заявка **#${id}** принята! Вы назначены поставщиком.`,
        [{label:'📌 Статус', value:`__sup_status_${id}`}, {label:'⬅️ К списку', value:'__sup_list'}]
      );
    }

    // Меню статусов поставщика
    if (text.startsWith('__sup_status_')) {
      const id = parseInt(text.replace('__sup_status_', ''));
      return b1Reply(`Выберите статус для заявки **#${id}**:`, [
        {label:'🔵 На сборке', value:`__sup_set_ASSEMBLING_${id}`},
        {label:'🟡 Задерживается', value:`__sup_set_DELAYED_${id}`},
        {label:'🚚 Отправлено доставкой', value:`__sup_ship_${id}`},
        {label:'🚗 Выезжаю на доставку', value:`__sup_onroute_${id}`},
        {label:'⬅️ Назад к заявке', value:`__view_sup_${id}`},
      ]);
    }

    // Простые статусы ASSEMBLING / DELAYED
    if (text.startsWith('__sup_set_ASSEMBLING_') || text.startsWith('__sup_set_DELAYED_')) {
      const isAssembling = text.startsWith('__sup_set_ASSEMBLING_');
      const id = parseInt(text.replace(isAssembling ? '__sup_set_ASSEMBLING_' : '__sup_set_DELAYED_', ''));
      const r = dlvGetReq(id);
      if (!r) return b1Reply(`❌ Заявка не найдена`);
      if (!r.supplier_id) { r.supplier_id = userId; r.supplier_name = u.name; }
      r.status = isAssembling ? 'ASSEMBLING' : 'DELAYED';
      r.status_by_name = u.name;
      dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:r.id});
      dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:r.id});
      const pretty = isAssembling ? '🔵 На сборке' : '🟡 Задерживается';
      // Уведомление менеджеру
      const mgrChatId = dlvGetChatId(r.created_by);
      if (mgrChatId && r.created_by !== userId) {
        setTimeout(() => {
          sendBotMsg(mgrChatId, `${pretty} — статус заявки **#${id}** обновлён. Поставщик: **${u.name}**.`, chatMemberIds(mgrChatId));
        }, 500);
      }
      return b1Reply(
        `✅ Статус заявки **#${id}** обновлён: ${pretty}.`,
        [{label:'⬅️ К заявке', value:`__view_sup_${id}`}]
      );
    }

    // Отправлено доставкой — запрос авто
    if (text.startsWith('__sup_ship_')) {
      const id = parseInt(text.replace('__sup_ship_', ''));
      b1State.set(userId, { step: 'ship_car_input', reqId: id });
      return b1Reply(`🚚 Заявка **#${id}**: введите номер и марку авто доставки:`);
    }
    if (state.step === 'ship_car_input') {
      const r = dlvGetReq(state.reqId);
      if (r) {
        if (!r.supplier_id) { r.supplier_id = userId; r.supplier_name = u.name; }
        r.status = 'SHIPPED'; r.car_info = text; r.status_by_name = u.name;
        dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:state.reqId});
        // Уведомление менеджеру
        const mgrChatId = dlvGetChatId(r.created_by);
        if (mgrChatId && r.created_by !== userId) {
          setTimeout(() => {
            sendBotMsg(mgrChatId, `🚚 **К вам выехала доставка** по заявке **#${state.reqId}**.\nПоставщик: **${u.name}**\nАвто: **${text}**`, chatMemberIds(mgrChatId));
          }, 500);
        }
      }
      b1State.set(userId, { step: 'menu' });
      return b1Reply(
        `✅ Статус заявки **#${state.reqId}**: 🚚 Отправлено доставкой.\nАвто: ${text}`,
        [{label:'⬅️ К заявке', value:`__view_sup_${state.reqId}`}]
      );
    }

    // Выезжаю — запрос времени прибытия
    if (text.startsWith('__sup_onroute_')) {
      const id = parseInt(text.replace('__sup_onroute_', ''));
      b1State.set(userId, { step: 'onroute_eta_input', reqId: id });
      return b1Reply(`🚗 Заявка **#${id}**: введите примерное время приезда (например: 40 минут / 18:30):`);
    }
    if (state.step === 'onroute_eta_input') {
      const r = dlvGetReq(state.reqId);
      if (r) {
        if (!r.supplier_id) { r.supplier_id = userId; r.supplier_name = u.name; }
        r.status = 'ON_ROUTE'; r.eta = text; r.status_by_name = u.name;
        dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:state.reqId});
        // Уведомление менеджеру
        const mgrChatId = dlvGetChatId(r.created_by);
        if (mgrChatId && r.created_by !== userId) {
          setTimeout(() => {
            sendBotMsg(mgrChatId, `🚗 **Выезжаю на доставку** по заявке **#${state.reqId}**.\nПоставщик: **${u.name}**\nВремя прибытия: **${text}**`, chatMemberIds(mgrChatId));
          }, 500);
        }
      }
      b1State.set(userId, { step: 'menu' });
      return b1Reply(
        `✅ Статус заявки **#${state.reqId}**: 🚗 Выезжаю.\nВремя прибытия: ${text}`,
        [{label:'⬅️ К заявке', value:`__view_sup_${state.reqId}`}]
      );
    }

    // Уточнить (вопрос поставщика менеджеру)
    if (text.startsWith('__sup_question_')) {
      const id = parseInt(text.replace('__sup_question_', ''));
      b1State.set(userId, { step: 'question_input', reqId: id });
      return b1Reply(`❓ Заявка **#${id}**: напишите уточнение менеджеру:`);
    }
    if (state.step === 'question_input') {
      const r = dlvGetReq(state.reqId);
      if (r) {
        if (!r.supplier_id) { r.supplier_id = userId; r.supplier_name = u.name; }
        r.status = 'QUESTION'; r.supplier_note = text;
        dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:state.reqId});
        // Уведомление менеджеру
        const mgrChatId = dlvGetChatId(r.created_by);
        if (mgrChatId && r.created_by !== userId) {
          setTimeout(() => {
            sendBotMsg(mgrChatId,
              `❓ Поставщик просит уточнение по заявке **#${state.reqId}**:\n\n${text}`,
              chatMemberIds(mgrChatId),
              [{label:'💬 Ответить поставщику', value:`__user_answer_req_${state.reqId}`}]
            );
          }, 500);
        }
      }
      b1State.set(userId, { step: 'menu' });
      return b1Reply(
        `❓ Уточнение по заявке **#${state.reqId}** отправлено менеджеру:\n\n${text}`,
        [{label:'🟦 Активные заявки', value:'__sup_list'}, {label:'🏠 Меню', value:'/start'}]
      );
    }

    // Отказ
    if (text.startsWith('__sup_reject_')) {
      const id = parseInt(text.replace('__sup_reject_', ''));
      const r = dlvGetReq(id);
      if (r) { r.status = 'REJECTED'; dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:r.id}); }
      return b1Reply(
        `❌ Заявка **#${id}** отклонена.`,
        [{label:'⬅️ К списку', value:'__sup_list'}, {label:'🏠 Меню', value:'/start'}]
      );
    }

    // Принять дополнение
    if (text.startsWith('__add_accept_')) {
      const addId = parseInt(text.replace('__add_accept_', ''));
      const a = dlvGetAdd(addId);
      if (a) { a.status = 'ACCEPTED'; dlvUpdateAdd(addId, a.status, a.supplier_note); }
      return b1Reply(`✅ Дополнение №${addId} принято.`, [{label:'🟦 Активные заявки', value:'__sup_list'}]);
    }

    // Отклонить дополнение
    if (text.startsWith('__add_reject_')) {
      const addId = parseInt(text.replace('__add_reject_', ''));
      const a = dlvGetAdd(addId);
      if (a) { a.status = 'REJECTED'; dlvUpdateAdd(addId, a.status, a.supplier_note); }
      return b1Reply(`❌ Дополнение №${addId}: не могу выполнить.`, [{label:'🟦 Активные заявки', value:'__sup_list'}]);
    }

    // Вопрос по дополнению
    if (text.startsWith('__add_question_')) {
      const addId = parseInt(text.replace('__add_question_', ''));
      b1State.set(userId, { step: 'add_question_input', addId });
      return b1Reply(`❓ Доп. №${addId}: напишите вопрос менеджеру:`);
    }
    if (state.step === 'add_question_input') {
      const a = dlvGetAdd(state.addId);
      if (a) dlvUpdateAdd(state.addId, 'QUESTION', text);
      b1State.set(userId, { step: 'menu' });
      return b1Reply(`❓ Вопрос по доп. №${state.addId} отправлен менеджеру.`, [{label:'🟦 Активные заявки', value:'__sup_list'}]);
    }

    if (state.step === 'menu') return b1MainMenu(userId);
  }

  // ══════════════════════════════════════════════════════════════
  // АДМИНИСТРАТОР
  // ══════════════════════════════════════════════════════════════
  if (u.role === 'ADMIN') {

    // Просмотр всех заявок
    if (text === '__adm_view') {
      const all = dlvAllReqs();
      if (!all.length) return b1Reply(`📭 Заявок пока нет.`, [{label:'🏠 Меню', value:'/start'}]);
      const btns = all.slice(-15).map(r => ({label: shortReqLine(r), value:`__adm_view_req_${r.id}`}));
      btns.push({label:'🏠 Меню', value:'/start'});
      return b1Reply(`👁 **Все заявки (${all.length}):**`, btns);
    }

    if (text.startsWith('__adm_view_req_')) {
      const id = parseInt(text.replace('__adm_view_req_', ''));
      const r = dlvGetReq(id);
      if (!r) return b1Reply(`❌ Заявка не найдена`, [{label:'⬅️ Назад', value:'__adm_view'}]);
      const btns = [{label:'⬅️ К списку', value:'__adm_view'}, {label:'🏠 Меню', value:'/start'}];
      if (r.status !== 'CLOSED') btns.unshift({label:'🔒 Закрыть', value:`__adm_close_${id}`});
      return b1Reply(fmtReq(r), btns);
    }

    if (text.startsWith('__adm_close_')) {
      const id = parseInt(text.replace('__adm_close_', ''));
      const r = dlvGetReq(id);
      if (r) { r.status = 'CLOSED'; dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:r.id}); }
      return b1Reply(`🔒 Заявка **#${id}** закрыта.`, [{label:'⬅️ К списку', value:'__adm_view'}, {label:'🏠 Меню', value:'/start'}]);
    }

    // Редактирование заявки
    if (text === '__adm_edit_start') {
      const all = dlvAllReqs();
      if (!all.length) return b1Reply(`Заявок нет.`, [{label:'🏠 Меню', value:'/start'}]);
      const btns = all.slice(-10).map(r => ({label:`#${r.id} — ${r.address?.substring(0,30)}`, value:`__adm_edit_pick_${r.id}`}));
      btns.push({label:'🏠 Меню', value:'/start'});
      return b1Reply(`✏️ **Редактирование заявок**\n\nВыберите заявку:`, btns);
    }

    if (text.startsWith('__adm_edit_pick_')) {
      const id = parseInt(text.replace('__adm_edit_pick_', ''));
      b1State.set(userId, { step: 'adm_edit_field', reqId: id });
      return b1Reply(`Заявка **#${id}** — выберите поле для редактирования:`, [
        {label:'Статус', value:`__adm_edit_field_status`},
        {label:'Адрес', value:`__adm_edit_field_address`},
        {label:'Контакт', value:`__adm_edit_field_contact`},
        {label:'Материалы', value:`__adm_edit_field_items`},
        {label:'Комментарий', value:`__adm_edit_field_comment`},
        {label:'⬅️ Назад', value:'__adm_edit_start'},
      ]);
    }

    if (state.step === 'adm_edit_field' && text.startsWith('__adm_edit_field_')) {
      const field = text.replace('__adm_edit_field_', '');
      b1State.set(userId, { ...state, step: 'adm_edit_value', field });
      return b1Reply(`Введите новое значение для поля **${field}**:`);
    }

    if (state.step === 'adm_edit_value') {
      const r = dlvGetReq(state.reqId);
      if (r) { r[state.field] = text; dlvUpdateReq({status:r.status,supplier_id:r.supplier_id,supplier_name:r.supplier_name,supplier_note:r.supplier_note,car_info:r.car_info,eta:r.eta,status_by_name:r.status_by_name,undelivered:r.undelivered,id:state.reqId}); }
      b1State.set(userId, { step: 'menu' });
      return b1Reply(`✅ Заявка **#${state.reqId}** обновлена.`, [{label:'🏠 Меню', value:'/start'}]);
    }

    // Список участников
    if (text === '__adm_users') {
      const all = dlvAllUsers();
      if (!all.length) return b1Reply(`👥 Нет участников.`, [{label:'🏠 Меню', value:'/start'}]);
      const roleIco = {MANAGER:'👷', SUPPLIER:'🚚', ADMIN:'🛂'};
      const lines = all.map(u => `${roleIco[u.role]||'👤'} **${u.name}** — ${u.role}`).join('\n');
      return b1Reply(`👥 **Участники (${all.length}):**\n\n${lines}`, [{label:'🏠 Меню', value:'/start'}]);
    }

    // Напоминания — меню
    if (text === '__adm_remind_menu') {
      return b1ReminderMenu();
    }

    if (text.startsWith('__adm_remind_mode_')) {
      const mode = text.replace('__adm_remind_mode_', '');
      if (['off','2h','daily'].includes(mode)) {
        reminderMode = mode;
        dlvSetSetting('reminder_mode', mode);
        scheduleReminders();
      }
      return b1ReminderMenu();
    }

    // Ручное напоминание — выбор поставщика
    if (text === '__adm_remind_now') {
      const sups = dlvSuppliers();
      if (!sups.length) return b1Reply(`Поставщиков пока нет.`, [{label:'⬅️ Назад', value:'__adm_remind_menu'}]);
      const btns = sups.map(s => ({label:`🚚 ${s.name}`, value:`__adm_remind_send_${s.user_id}`}));
      btns.push({label:'⬅️ Назад', value:'__adm_remind_menu'});
      return b1Reply(`Выберите поставщика для ручного напоминания:`, btns);
    }

    if (text.startsWith('__adm_remind_send_')) {
      const supId = text.replace('__adm_remind_send_', '');
      const supChatId = dlvGetChatId(supId);
      if (supChatId) {
        const reminderText = buildReminderText(supId, true);
        setTimeout(() => sendBotMsg(supChatId, reminderText, chatMemberIds(supChatId), [{label:'🟦 Активные заявки', value:'__sup_list'}]), 300);
      }
      return b1Reply(`✅ Напоминание отправлено поставщику.`, [{label:'⬅️ Назад', value:'__adm_remind_menu'}]);
    }

    if (state.step === 'menu') return b1MainMenu(userId);
  }

  b1State.set(userId, { step: 'menu' });
  return b1MainMenu(userId);
}

function b1ReminderMenu() {
  const mark = (m) => reminderMode === m ? '✅ ' : '';
  return b1Reply(`🔔 **Управление напоминаниями поставщикам**\n\nВыберите режим:`, [
    {label:`${mark('2h')}Каждые 2 часа (Пн–Пт 08:00–19:00 МСК)`, value:'__adm_remind_mode_2h'},
    {label:`${mark('daily')}1 раз в день (Пн–Пт 10:00 МСК)`, value:'__adm_remind_mode_daily'},
    {label:`${mark('off')}Отключить напоминания`, value:'__adm_remind_mode_off'},
    {label:'📣 Отправить сейчас (вручную)', value:'__adm_remind_now'},
  ]);
}

// ── Напоминания поставщикам (расписание) ─────────────────────────────────────
function buildReminderText(supId, manual) {
  const myReqs = dlvAllReqs().filter(r =>
    (r.supplier_id === supId || (!r.supplier_id && r.status === 'SENT')) &&
    !['CLOSED','REJECTED'].includes(r.status)
  );
  const title = manual ? '🔔 **Ручное напоминание: проверь заявки на доставку**' : '⏰ **Авто-напоминание: проверь заявки на доставку**';
  if (myReqs.length) {
    const lines = myReqs.slice(0,20).map(r => `• ${shortReqLine(r)}`).join('\n');
    return `${title}\n\n${lines}\n\nОткрой: 🟦 Активные заявки`;
  }
  return `${title}\n\nОткрой: 🟦 Активные заявки`;
}

function scheduleReminders() {
  // Очищаем старые таймеры
  for (const t of reminderTimers) clearInterval(t);
  reminderTimers = [];

  if (reminderMode === 'off') return;

  const now = new Date();
  const mskOffset = 3 * 60 * 60 * 1000; // UTC+3

  function nextFireMs(targetHourMSK) {
    const mskNow = new Date(Date.now() + mskOffset);
    const mskToday = new Date(mskNow);
    mskToday.setUTCHours(targetHourMSK, 0, 0, 0);
    let ms = mskToday - mskNow;
    if (ms <= 0) ms += 24 * 60 * 60 * 1000;
    return ms;
  }

  function fireReminderToAllSuppliers() {
    const mskNow = new Date(Date.now() + mskOffset);
    const day = mskNow.getUTCDay(); // 0=Sun, 6=Sat
    if (day === 0 || day === 6) return; // только Пн-Пт
    const h = mskNow.getUTCHours();
    if (reminderMode === '2h' && (h < 8 || h > 18)) return;
    if (reminderMode === 'daily' && h !== 10) return;

    const sups = dlvSuppliers();
    for (const sup of sups) {
      const supId = sup.user_id;
      const chatId = dlvGetChatId(supId);
      if (!chatId) continue;
      const text = buildReminderText(supId, false);
      const memberIds = chatMemberIds(chatId);
      sendBotMsg(chatId, text, memberIds, [{label:'🟦 Активные заявки', value:'__sup_list'}]);
    }
  }

  if (reminderMode === '2h') {
    // Каждые 2 часа с 8 до 18 МСК, Пн-Пт
    const hours = [8, 10, 12, 14, 16, 18];
    for (const h of hours) {
      const ms = nextFireMs(h);
      const t = setTimeout(() => {
        fireReminderToAllSuppliers();
        const daily = setInterval(fireReminderToAllSuppliers, 24 * 60 * 60 * 1000);
        reminderTimers.push(daily);
      }, ms);
      reminderTimers.push(t);
    }
  } else if (reminderMode === 'daily') {
    const ms = nextFireMs(10);
    const t = setTimeout(() => {
      fireReminderToAllSuppliers();
      const daily = setInterval(fireReminderToAllSuppliers, 24 * 60 * 60 * 1000);
      reminderTimers.push(daily);
    }, ms);
    reminderTimers.push(t);
  }
}

// Запускаем расписание при старте
scheduleReminders();


// ─── BOT2 (@karnizcal) — Калькулятор карнизов ───────────────────────────────
const OFFSET_STRAIGHT_CENTER = 15.2;
const OFFSET_STRAIGHT_LTR = 11.6;
const OFFSET_L_A = 21.15;
const OFFSET_L_B = 17.45;
const MAX_SECTION_LEN = 310.0;

function parseCm(text) {
  const s = text.trim().replace(/\s/g,'').replace(',','.');
  const n = parseFloat(s);
  if (isNaN(n) || n <= 0) throw new Error('bad');
  return n;
}
function evenUp(n) { return n % 2 === 0 ? n : n + 1; }
function splitSections(total) {
  if (total <= 0) return [0];
  const n = Math.max(1, Math.ceil(total / MAX_SECTION_LEN));
  return Array(n).fill(total / n);
}
function fmtPieces(arr, dec=1) {
  return arr.map(v => v.toFixed(dec)).join(' + ');
}
function calcStraight(x, mode) {
  const offset = mode === 'center' ? OFFSET_STRAIGHT_CENTER : OFFSET_STRAIGHT_LTR;
  const xEff = Math.max(0, x - offset);
  const pieces = splitSections(xEff);
  const runners = evenUp(Math.ceil(x / 8));
  return { xEff, pieces, runners, hooks: runners + 10, mounts: Math.ceil(x / 100) + 1 };
}
function calcL(x, y, mode) {
  const [xOff, yOff] = mode === 'rtl' ? [OFFSET_L_B, OFFSET_L_A] : [OFFSET_L_A, OFFSET_L_B];
  const xEff = Math.max(0, x - xOff);
  const yEff = Math.max(0, y - yOff);
  const runners = evenUp(Math.ceil((x + y) / 8));
  return { xEff, yEff, piecesX: splitSections(xEff), piecesY: splitSections(yEff), runners, hooks: runners + 10, mounts: Math.ceil(x/100) + Math.ceil(y/100) + 2 };
}

const b2State = new Map();

function b2Reply(text, buttons) { return {text, buttons: buttons||null}; }

function processBot2(text, userId) {
  const state = b2State.get(userId) || { step: 'menu' };
  text = (text || '').trim();

  if (text === '/start' || text === '/new' || text === '/menu' || text === '__b2_menu') {
    b2State.set(userId, { step: 'menu' });
    return b2Reply(
      `📐 **КарнизКал** — Калькулятор карнизов\n\nВыберите тип карниза:`,
      [{label:'📏 Прямой карниз', value:'__b2_straight'}, {label:'📐 Г-образный карниз', value:'__b2_l'}]
    );
  }
  if (text === '/help') return b2Reply(
    `📐 **КарнизКал** — Помощь\n\n📏 Прямой — для одной стены\n📐 Г-образный — для угловой комнаты\n\nВведите размеры в сантиметрах, например: **510** или **510,5**\n\nКоманды:\n/start — главное меню\n/help — эта справка\n/stop — сбросить сессию`,
    [{label:'🏠 Главное меню', value:'__b2_menu'}]
  );
  if (text === '/stop') {
    b2State.delete(userId);
    return b2Reply(`✋ Сессия сброшена.`, [{label:'🏠 Начать', value:'__b2_menu'}]);
  }

  if (text === '__b2_straight') {
    b2State.set(userId, { step: 'straight_mode' });
    return b2Reply(
      `📏 **Прямой карниз**\n\nВыберите режим раздвижения:`,
      [{label:'↔️ К центру', value:'__b2_sm_center'}, {label:'➡️ Слева-Направо', value:'__b2_sm_ltr'}, {label:'⬅️ Справа-Налево', value:'__b2_sm_rtl'}, {label:'🏠 Назад', value:'__b2_menu'}]
    );
  }
  if (text === '__b2_l') {
    b2State.set(userId, { step: 'l_mode' });
    return b2Reply(
      `📐 **Г-образный карниз**\n\nВыберите режим раздвижения:`,
      [{label:'↔️ К центру', value:'__b2_lm_center'}, {label:'➡️ Слева-Направо', value:'__b2_lm_ltr'}, {label:'⬅️ Справа-Налево', value:'__b2_lm_rtl'}, {label:'⬅️ Назад', value:'__b2_menu'}]
    );
  }

  if (state.step === 'straight_mode' || text.startsWith('__b2_sm_')) {
    const modeMap = {'__b2_sm_center':'center','__b2_sm_ltr':'ltr','__b2_sm_rtl':'rtl'};
    const modeNames = {center:'К центру', ltr:'Слева-Направо', rtl:'Справа-Налево'};
    const mode = modeMap[text];
    if (!mode) return b2Reply(`Выберите режим:`, [{label:'↔️ К центру', value:'__b2_sm_center'},{label:'➡️ Слева-Направо', value:'__b2_sm_ltr'},{label:'⬅️ Справа-Налево', value:'__b2_sm_rtl'},{label:'🏠 Назад', value:'__b2_menu'}]);
    b2State.set(userId, { step: 'straight_len', mode, modeName: modeNames[mode] });
    return b2Reply(`📏 Режим: **${modeNames[mode]}**\n\nВведите длину карниза X (см):\n_Например: 510_`, [{label:'🏠 Назад', value:'__b2_straight'}]);
  }

  if (state.step === 'straight_len') {
    try {
      const x = parseCm(text);
      const r = calcStraight(x, state.mode);
      b2State.set(userId, { step: 'menu' });
      return b2Reply(
        `✅ **Прямой карниз**\nРежим: **${state.modeName}**\nДлина X: **${x.toFixed(0)} см**\n\nПосле вычета: **${r.xEff.toFixed(1)} см**\nСекции: **${fmtPieces(r.pieces)} см**\n\nБегунков: **${r.runners} шт.**\nКрючков: **${r.hooks} шт.**\nКреплений: **${r.mounts} шт.**`,
        [{label:'🔁 Новый расчёт', value:'__b2_menu'}, {label:'📏 Ещё прямой', value:'__b2_straight'}]
      );
    } catch(e) {
      return b2Reply(`❌ Не понял размер. Введите число в сантиметрах, например: **510**`);
    }
  }

  if (state.step === 'l_mode' || text.startsWith('__b2_lm_')) {
    const modeMap = {'__b2_lm_center':'center','__b2_lm_ltr':'ltr','__b2_lm_rtl':'rtl'};
    const modeNames = {center:'К центру', ltr:'Слева-Направо', rtl:'Справа-Налево'};
    const mode = modeMap[text];
    if (!mode) return b2Reply(`Выберите режим:`, [{label:'↔️ К центру', value:'__b2_lm_center'},{label:'➡️ Слева-Направо', value:'__b2_lm_ltr'},{label:'⬅️ Справа-Налево', value:'__b2_lm_rtl'}]);
    b2State.set(userId, { step: 'l_len_x', mode, modeName: modeNames[mode] });
    return b2Reply(`📐 Режим: **${modeNames[mode]}**\n\nВведите длину X (см):\n_Например: 640_`);
  }

  if (state.step === 'l_len_x') {
    try {
      const x = parseCm(text);
      b2State.set(userId, { ...state, step: 'l_len_y', x });
      return b2Reply(`✅ X = **${x.toFixed(0)} см**\n\nТеперь введите длину Y (см):\n_Например: 280_`);
    } catch(e) {
      return b2Reply(`❌ Не понял размер X. Введите число, например: **640**`);
    }
  }

  if (state.step === 'l_len_y') {
    try {
      const y = parseCm(text);
      const r = calcL(state.x, y, state.mode);
      b2State.set(userId, { step: 'menu' });
      return b2Reply(
        `✅ **Г-образный карниз**\nРежим: **${state.modeName}**\n\nX: **${state.x.toFixed(0)} см** → **${r.xEff.toFixed(2)} см**\nY: **${y.toFixed(0)} см** → **${r.yEff.toFixed(2)} см**\n\nСекции X: **${fmtPieces(r.piecesX, 2)} см**\nСекции Y: **${fmtPieces(r.piecesY, 2)} см**\n\nБегунков: **${r.runners} шт.**\nКрючков: **${r.hooks} шт.**\nКреплений: **${r.mounts} шт.**`,
        [{label:'🔁 Новый расчёт', value:'__b2_menu'}, {label:'📐 Ещё Г-образный', value:'__b2_l'}]
      );
    } catch(e) {
      return b2Reply(`❌ Не понял размер Y. Введите число, например: **280**`);
    }
  }

  b2State.set(userId, { step: 'menu' });
  return b2Reply(`📐 **КарнизКал**\n\nВыберите тип карниза:`,
    [{label:'📏 Прямой карниз', value:'__b2_straight'}, {label:'📐 Г-образный карниз', value:'__b2_l'}]
  );
}

// ─── DB queries ──────────────────────────────────────────────────────────────
const q = {
  createUser:     db.prepare('INSERT INTO users(id,name,username,password,avatar_color) VALUES(?,?,?,?,?)'),
  userByUsername: db.prepare('SELECT * FROM users WHERE username=?'),
  userById:       db.prepare('SELECT id,name,username,avatar_color,avatar_data,last_seen,is_bot FROM users WHERE id=?'),
  searchUsers:    db.prepare("SELECT id,name,username,avatar_color,avatar_data,is_bot FROM users WHERE (username LIKE ? OR name LIKE ?) LIMIT 20"),
  touchUser:      db.prepare('UPDATE users SET last_seen=? WHERE id=?'),
  addSession:     db.prepare('INSERT INTO sessions(token,user_id) VALUES(?,?)'),
  getSession:     db.prepare('SELECT s.token, u.id,u.name,u.username,u.avatar_color,u.avatar_data,u.is_bot FROM sessions s JOIN users u ON s.user_id=u.id WHERE s.token=?'),
  delSession:     db.prepare('DELETE FROM sessions WHERE token=?'),
  updatePassword: db.prepare('UPDATE users SET password=? WHERE id=?'),
  updateAvatar:   db.prepare('UPDATE users SET avatar_data=? WHERE id=?'),
  createChat:     db.prepare('INSERT INTO chats(id,type,name,created_by) VALUES(?,?,?,?)'),
  chatById:       db.prepare('SELECT * FROM chats WHERE id=?'),
  directChat:     db.prepare(`SELECT c.* FROM chats c JOIN chat_members a ON c.id=a.chat_id AND a.user_id=? JOIN chat_members b ON c.id=b.chat_id AND b.user_id=? WHERE c.type='direct' LIMIT 1`),
  userChats:      db.prepare(`SELECT c.*,(SELECT text FROM messages WHERE chat_id=c.id AND deleted=0 ORDER BY created_at DESC LIMIT 1) as last_text,(SELECT type FROM messages WHERE chat_id=c.id AND deleted=0 ORDER BY created_at DESC LIMIT 1) as last_type,(SELECT created_at FROM messages WHERE chat_id=c.id AND deleted=0 ORDER BY created_at DESC LIMIT 1) as last_at FROM chats c JOIN chat_members cm ON c.id=cm.chat_id WHERE cm.user_id=? ORDER BY COALESCE(last_at,c.created_at) DESC`),
  chatMembers:    db.prepare('SELECT u.id,u.name,u.username,u.avatar_color,u.avatar_data,u.is_bot,cm.is_admin FROM users u JOIN chat_members cm ON u.id=cm.user_id WHERE cm.chat_id=?'),
  addMember:      db.prepare('INSERT OR IGNORE INTO chat_members(chat_id,user_id,is_admin) VALUES(?,?,?)'),
  removeMember:   db.prepare('DELETE FROM chat_members WHERE chat_id=? AND user_id=?'),
  isMember:       db.prepare('SELECT 1 FROM chat_members WHERE chat_id=? AND user_id=?'),
  isAdmin:        db.prepare('SELECT is_admin FROM chat_members WHERE chat_id=? AND user_id=?'),
  chatMemberIds:  db.prepare('SELECT user_id FROM chat_members WHERE chat_id=?'),
  insertMsg:      db.prepare('INSERT INTO messages(id,chat_id,from_id,type,text,media_data,media_name,media_size,media_duration,reply_to) VALUES(?,?,?,?,?,?,?,?,?,?)'),
  chatMsgs:       db.prepare('SELECT * FROM messages WHERE chat_id=? AND deleted=0 ORDER BY created_at ASC LIMIT 200'),
  getMsg:         db.prepare('SELECT * FROM messages WHERE id=?'),
  editMsg:        db.prepare('UPDATE messages SET text=?,edited=1 WHERE id=? AND from_id=?'),
  delMsg:         db.prepare('UPDATE messages SET deleted=1 WHERE id=? AND from_id=?'),
  updateChatName: db.prepare('UPDATE chats SET name=? WHERE id=?'),
  updateChatAvatar: db.prepare('UPDATE chats SET avatar_data=? WHERE id=?'),
};

const COLORS = ['#6C3AE8','#E83A6C','#3A8EE8','#E8923A','#3AE87A','#9B5DEA','#E83AE0','#3AE8E8'];
const randColor = () => COLORS[Math.floor(Math.random()*COLORS.length)];

const conns = new Map();
function broadcast(payload, userIds) {
  const data = JSON.stringify(payload);
  for (const uid of userIds) {
    const socks = conns.get(uid);
    if (!socks) continue;
    for (const ws of socks) {
      try { if (ws.readyState === 1) ws.send(data); } catch(e) {}
    }
  }
}
function chatMemberIds(chatId) {
  return q.chatMemberIds.all(chatId).map(r => r.user_id);
}
function getAuth(req) {
  const token = (req.headers.authorization||'').replace('Bearer ','').trim();
  if (!token) return null;
  const s = q.getSession.get(token);
  if (!s) return null;
  q.touchUser.run(Date.now(), s.id);
  return { ...s, token };
}
function apiErr(res, code, msg) { res.writeHead(code); res.end(JSON.stringify({ error: msg })); }
function apiOk(res, data) { res.writeHead(200); res.end(JSON.stringify(data)); }

// buttons: [{label:'текст', value:'команда'}, ...]
function sendBotMsg(chatId, text, memberIds, buttons) {
  const id = uuid(); const now = Date.now();
  q.insertMsg.run(id, chatId, BOT1_ID, 'text', text, null, null, null, null, null);
  const msg = {id,chat_id:chatId,from_id:BOT1_ID,type:'text',text,media_data:null,media_name:null,media_size:null,media_duration:null,reply_to:null,edited:0,deleted:0,created_at:now,buttons:buttons||null};
  broadcast({type:'new_message',message:msg}, memberIds);
}
function sendBotMsg2(chatId, text, memberIds, buttons) {
  const id = uuid(); const now = Date.now();
  q.insertMsg.run(id, chatId, BOT2_ID, 'text', text, null, null, null, null, null);
  const msg = {id,chat_id:chatId,from_id:BOT2_ID,type:'text',text,media_data:null,media_name:null,media_size:null,media_duration:null,reply_to:null,edited:0,deleted:0,created_at:now,buttons:buttons||null};
  broadcast({type:'new_message',message:msg}, memberIds);
}

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  const urlObj = new URL(req.url||'/', 'http://x');
  const pathname = urlObj.pathname;

  if (pathname === '/manifest.json') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({
      name: 'Голубь', short_name: 'Голубь',
      description: 'Голубь Мессенджер',
      start_url: '/', display: 'standalone',
      background_color: '#0D0D14', theme_color: '#5B5EF4',
      icons: [
        { src: '/icons/icon-192.png', sizes: '192x192', type: 'image/png' },
        { src: '/icons/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' }
      ]
    }));
    return;
  }

  if (pathname.startsWith('/api/')) {
    res.setHeader('Content-Type','application/json');
    res.setHeader('Access-Control-Allow-Origin','*');
    res.setHeader('Access-Control-Allow-Headers','Content-Type,Authorization');
    res.setHeader('Access-Control-Allow-Methods','GET,POST,PUT,DELETE,OPTIONS,PATCH');
    if (req.method==='OPTIONS'){res.writeHead(200);res.end();return;}
    let body='';
    req.on('data',d=>{body+=d; if(body.length>20*1024*1024){res.writeHead(413);res.end();}});
    req.on('end',()=>{
      let data={};
      try{if(body)data=JSON.parse(body);}catch(e){}
      handleAPI(req, res, pathname, urlObj, data);
    });
    return;
  }

  const safePath = pathname.replace(/\.\./g,'');
  const fp = path.join(__dirname,'public', safePath==='/'?'index.html':safePath);
  const ext = path.extname(fp);
  const mime = {'.html':'text/html','.js':'application/javascript','.css':'text/css','.png':'image/png','.ico':'image/x-icon'}[ext]||'text/plain';
  fs.readFile(fp, (err, d) => {
    if (err) {
      fs.readFile(path.join(__dirname,'public','index.html'), (e2,d2) => {
        if(e2){res.writeHead(404);res.end('Not Found');return;}
        res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Permissions-Policy':'microphone=*, camera=*','Cache-Control':'no-cache'});
        res.end(d2);
      });
      return;
    }
    res.writeHead(200,{'Content-Type':mime+(mime.includes('text')?'; charset=utf-8':''),'Cache-Control':ext==='.html'?'no-cache':'max-age=3600'});
    res.end(d);
  });
});

function handleAPI(req, res, pathname, urlObj, data) {
  if (pathname==='/api/register' && req.method==='POST') {
    const {name,username,password} = data;
    if (!name?.trim()||!username?.trim()||!password) return apiErr(res,400,'Заполните все поля');
    if (password.length<4) return apiErr(res,400,'Пароль минимум 4 символа');
    if (!/^[a-zA-Z0-9_]{2,30}$/.test(username)) return apiErr(res,400,'Логин: только буквы, цифры и _');
    if (q.userByUsername.get(username)) return apiErr(res,409,'Логин уже занят');
    const hash = bcrypt.hashSync(password, 10);
    const id = uuid(); const color = randColor();
    q.createUser.run(id, name.trim(), username.trim(), hash, color);
    const token = uuid(); q.addSession.run(token, id);
    return apiOk(res,{token, user:{id, name:name.trim(), username:username.trim(), avatar_color:color, avatar_data:null, is_bot:0}});
  }

  if (pathname==='/api/login' && req.method==='POST') {
    const {username,password} = data;
    if (!username||!password) return apiErr(res,400,'Заполните все поля');
    const user = q.userByUsername.get(username);
    if (!user||!bcrypt.compareSync(password,user.password)) return apiErr(res,401,'Неверный логин или пароль');
    const token = uuid(); q.addSession.run(token, user.id);
    return apiOk(res,{token, user:{id:user.id, name:user.name, username:user.username, avatar_color:user.avatar_color, avatar_data:user.avatar_data||null, is_bot:user.is_bot||0}});
  }

  if (pathname==='/api/logout' && req.method==='POST') {
    const me = getAuth(req); if (me) q.delSession.run(me.token);
    return apiOk(res,{ok:true});
  }

  if (pathname==='/api/me' && req.method==='GET') {
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    return apiOk(res,{user:{id:me.id,name:me.name,username:me.username,avatar_color:me.avatar_color,avatar_data:me.avatar_data||null,is_bot:me.is_bot||0}});
  }

  if (pathname==='/api/me/password' && req.method==='POST') {
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    const {old_password,new_password} = data;
    if (!old_password||!new_password) return apiErr(res,400,'Заполните все поля');
    if (new_password.length<4) return apiErr(res,400,'Минимум 4 символа');
    const user = q.userByUsername.get(me.username);
    if (!bcrypt.compareSync(old_password,user.password)) return apiErr(res,400,'Неверный текущий пароль');
    q.updatePassword.run(bcrypt.hashSync(new_password,10),me.id);
    return apiOk(res,{ok:true});
  }

  if (pathname==='/api/me/avatar' && req.method==='POST') {
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    const {avatar_data} = data; if (!avatar_data) return apiErr(res,400,'Нет данных');
    q.updateAvatar.run(avatar_data, me.id);
    const chats = q.userChats.all(me.id);
    const contacts = new Set([me.id]);
    chats.forEach(c => q.chatMemberIds.all(c.id).forEach(r => contacts.add(r.user_id)));
    broadcast({type:'user_avatar',user_id:me.id,avatar_data},[...contacts]);
    return apiOk(res,{ok:true,avatar_data});
  }

  if (pathname==='/api/users/search' && req.method==='GET') {
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    const pat = (urlObj.searchParams.get('q')||'')+'%';
    const users = q.searchUsers.all(pat,pat).filter(u=>u.id!==me.id);
    const qStr = (urlObj.searchParams.get('q')||'').toLowerCase();
    // Always include bots in search
    for (const [bid, bun, bname] of [[BOT1_ID,BOT1_USERNAME,BOT1_NAME],[BOT2_ID,BOT2_USERNAME,BOT2_NAME]]) {
      if (!users.some(u=>u.id===bid) && (qStr===''||bun.includes(qStr)||bname.toLowerCase().includes(qStr))) {
        const bu = q.userById.get(bid);
        if (bu) users.unshift({id:bu.id,name:bu.name,username:bu.username,avatar_color:bu.avatar_color,avatar_data:bu.avatar_data,is_bot:1});
      }
    }
    return apiOk(res,{users});
  }

  if (pathname==='/api/chats' && req.method==='GET') {
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    return apiOk(res,{chats: q.userChats.all(me.id).map(c=>({...c,members:q.chatMembers.all(c.id)}))});
  }

  if (pathname==='/api/chats' && req.method==='POST') {
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    const {type,members=[],name} = data;
    if (type==='direct') {
      if (!members[0]) return apiErr(res,400,'Нужен участник');
      const otherId = members[0];
      const existing = q.directChat.get(me.id,otherId);
      if (existing) return apiOk(res,{chat:{...existing,members:q.chatMembers.all(existing.id)},messages:q.chatMsgs.all(existing.id)});
      const id = uuid(); q.createChat.run(id,'direct',null,me.id);
      q.addMember.run(id,me.id,0); q.addMember.run(id,otherId,0);
      const chat = {...q.chatById.get(id),members:q.chatMembers.all(id)};
      broadcast({type:'new_chat',chat},[otherId]);
      // Bot welcome
      if (otherId === BOT1_ID) {
        b1State.set(me.id, { step: 'menu' });
        setTimeout(()=>{
          const r=processBot1('/start', me.id, id);
          const {text:bt,buttons:bb}=(typeof r==='object'&&r.text!==undefined)?r:{text:r,buttons:null};
          if(bt)sendBotMsg(id,bt,[me.id,BOT1_ID],bb);
        },500);
      }
      if (otherId === BOT2_ID) {
        b2State.set(me.id, {step:'menu'});
        setTimeout(()=>{
          const r=processBot2('/start', me.id);
          const {text:bt,buttons:bb}=(typeof r==='object'&&r.text!==undefined)?r:{text:r,buttons:null};
          if(bt)sendBotMsg2(id,bt,[me.id,BOT2_ID],bb);
        },500);
      }
      return apiOk(res,{chat,messages:[]});
    }
    if (type==='group') {
      if (!name?.trim()) return apiErr(res,400,'Нужно название');
      const id = uuid(); q.createChat.run(id,'group',name.trim(),me.id);
      q.addMember.run(id,me.id,1);
      members.forEach(uid=>q.addMember.run(id,uid,0));
      const chat = {...q.chatById.get(id),members:q.chatMembers.all(id)};
      broadcast({type:'new_chat',chat},members);
      return apiOk(res,{chat,messages:[]});
    }
    return apiErr(res,400,'Неверный тип');
  }

  // Update group name/avatar
  const chatEditMatch = pathname.match(/^\/api\/chats\/([^/]+)$/);
  if (chatEditMatch && req.method==='PATCH') {
    const chatId = chatEditMatch[1];
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    const chat = q.chatById.get(chatId);
    if (!chat||chat.type!=='group') return apiErr(res,404,'Группа не найдена');
    const adminRow = q.isAdmin.get(chatId,me.id);
    if (!adminRow||!adminRow.is_admin) return apiErr(res,403,'Только администратор');
    if (data.name !== undefined) {
      if (!data.name?.trim()) return apiErr(res,400,'Название не может быть пустым');
      q.updateChatName.run(data.name.trim(), chatId);
    }
    if (data.avatar_data !== undefined) {
      q.updateChatAvatar.run(data.avatar_data, chatId);
    }
    const updatedChat = {...q.chatById.get(chatId), members: q.chatMembers.all(chatId)};
    broadcast({type:'chat_updated',chat:updatedChat}, chatMemberIds(chatId));
    return apiOk(res,{ok:true, chat:updatedChat});
  }

  // Add member to group
  const addMemberMatch = pathname.match(/^\/api\/chats\/([^/]+)\/members$/);
  if (addMemberMatch && req.method==='POST') {
    const chatId = addMemberMatch[1];
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    const chat = q.chatById.get(chatId);
    if (!chat||chat.type!=='group') return apiErr(res,404,'Группа не найдена');
    const adminRow = q.isAdmin.get(chatId,me.id);
    if (!adminRow||!adminRow.is_admin) return apiErr(res,403,'Только администратор');
    const {user_id} = data;
    if (!user_id) return apiErr(res,400,'Нужен user_id');
    q.addMember.run(chatId,user_id,0);
    const updatedChat = {...q.chatById.get(chatId),members:q.chatMembers.all(chatId)};
    const memberIds = chatMemberIds(chatId);
    broadcast({type:'chat_updated',chat:updatedChat},memberIds);
    const addedUser = q.userById.get(user_id);
    if (addedUser) sendBotMsg(chatId,`👋 ${addedUser.name} присоединился к группе`,memberIds);
    return apiOk(res,{ok:true,chat:updatedChat});
  }

  // Remove member
  const removeMemberMatch = pathname.match(/^\/api\/chats\/([^/]+)\/members\/([^/]+)$/);
  if (removeMemberMatch && req.method==='DELETE') {
    const chatId = removeMemberMatch[1], targetId = removeMemberMatch[2];
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    const chat = q.chatById.get(chatId);
    if (!chat||chat.type!=='group') return apiErr(res,404,'Группа не найдена');
    if (me.id !== targetId) {
      const adminRow = q.isAdmin.get(chatId,me.id);
      if (!adminRow||!adminRow.is_admin) return apiErr(res,403,'Только администратор');
    }
    if (targetId===chat.created_by&&me.id!==targetId) return apiErr(res,403,'Нельзя удалить создателя');
    const removedUser = q.userById.get(targetId);
    q.removeMember.run(chatId,targetId);
    const memberIds = chatMemberIds(chatId);
    const updatedChat = {...q.chatById.get(chatId),members:q.chatMembers.all(chatId)};
    broadcast({type:'chat_updated',chat:updatedChat},memberIds);
    broadcast({type:'removed_from_chat',chat_id:chatId},[targetId]);
    if (removedUser) sendBotMsg(chatId,`👋 ${removedUser.name} покинул группу`,memberIds);
    return apiOk(res,{ok:true});
  }

  const cmMatch = pathname.match(/^\/api\/chats\/([^/]+)\/messages$/);
  if (cmMatch) {
    const chatId = cmMatch[1];
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    if (!q.isMember.get(chatId,me.id)) return apiErr(res,403,'Нет доступа');
    if (req.method==='GET') return apiOk(res,{messages:q.chatMsgs.all(chatId)});
    if (req.method==='POST') {
      const {type='text',text,media_data,media_name,media_size,media_duration,reply_to} = data;
      if (type==='text'&&!text?.trim()) return apiErr(res,400,'Пустое сообщение');
      const id = uuid(); const now = Date.now();
      q.insertMsg.run(id,chatId,me.id,type,text||null,media_data||null,media_name||null,media_size||null,media_duration||null,reply_to||null);
      const msg = {id,chat_id:chatId,from_id:me.id,type,text:text||null,media_data:media_data||null,media_name:media_name||null,media_size:media_size||null,media_duration:media_duration||null,reply_to:reply_to||null,edited:0,deleted:0,created_at:now};
      const memberIds = chatMemberIds(chatId);
      broadcast({type:'new_message',message:msg},memberIds);

      // Bot1 (@ditryexpress)
      if (type==='text'&&text) {
        const members = q.chatMembers.all(chatId);
        if (members.some(m=>m.id===BOT1_ID)) {
          const resp = processBot1(text, me.id, chatId);
          if (resp) {
            const {text:bt, buttons:bb} = (typeof resp==='object'&&resp.text!==undefined) ? resp : {text:resp, buttons:null};
            if (bt) setTimeout(()=>sendBotMsg(chatId,bt,memberIds,bb),400);
          }
        }
        // Bot2 (@karnizcal)
        if (members.some(m=>m.id===BOT2_ID)) {
          const resp2 = processBot2(text, me.id);
          if (resp2) {
            const {text:bt, buttons:bb} = (typeof resp2==='object'&&resp2.text!==undefined) ? resp2 : {text:resp2, buttons:null};
            if (bt) setTimeout(()=>sendBotMsg2(chatId,bt,memberIds,bb),400);
          }
        }
      }
      return apiOk(res,{message:msg});
    }
  }

  const msgMatch = pathname.match(/^\/api\/messages\/([^/]+)$/);
  if (msgMatch) {
    const msgId = msgMatch[1];
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    const msg = q.getMsg.get(msgId); if (!msg) return apiErr(res,404,'Не найдено');
    if (!q.isMember.get(msg.chat_id,me.id)) return apiErr(res,403,'Нет доступа');
    if (req.method==='PUT') {
      const {text} = data; if (!text?.trim()) return apiErr(res,400,'Пустой текст');
      if (msg.from_id!==me.id) return apiErr(res,403,'Нельзя редактировать чужие');
      q.editMsg.run(text.trim(),msgId,me.id);
      const updated = q.getMsg.get(msgId);
      broadcast({type:'edit_message',message:updated},chatMemberIds(msg.chat_id));
      return apiOk(res,{ok:true,message:updated});
    }
    if (req.method==='DELETE') {
      if (msg.from_id!==me.id) {
        const chat = q.chatById.get(msg.chat_id);
        if (chat?.type==='group') {
          const adminRow = q.isAdmin.get(msg.chat_id,me.id);
          if (!adminRow||!adminRow.is_admin) return apiErr(res,403,'Нет прав');
        } else return apiErr(res,403,'Нельзя');
      }
      q.delMsg.run(msgId,me.id);
      broadcast({type:'delete_message',message_id:msgId,chat_id:msg.chat_id},chatMemberIds(msg.chat_id));
      return apiOk(res,{ok:true});
    }
  }

  // Online status endpoint
  if (pathname==='/api/users/online' && req.method==='GET') {
    const me = getAuth(req); if (!me) return apiErr(res,401,'Не авторизован');
    const ids = (urlObj.searchParams.get('ids')||'').split(',').filter(Boolean);
    const result = {};
    ids.forEach(id => { result[id] = conns.has(id); });
    return apiOk(res, {online: result});
  }

  apiErr(res,404,'Not found');
}

const wss = new WebSocketServer({server});
wss.on('connection', ws => {
  let userId = null;
  ws.on('message', raw => {
    let msg; try { msg = JSON.parse(raw); } catch(e) { return; }
    if (msg.type==='auth') {
      const s = q.getSession.get(msg.token);
      if (!s) { ws.send(JSON.stringify({type:'auth_fail'})); return; }
      userId = s.id;
      if (!conns.has(userId)) conns.set(userId, new Set());
      conns.get(userId).add(ws);
      q.touchUser.run(Date.now(), userId);
      ws.send(JSON.stringify({type:'auth_ok',user_id:userId}));
      const chats = q.userChats.all(userId);
      const contacts = new Set();
      chats.forEach(c => q.chatMemberIds.all(c.id).forEach(r => { if(r.user_id!==userId) contacts.add(r.user_id); }));
      broadcast({type:'user_online',user_id:userId},[...contacts]);
      return;
    }
    if (msg.type==='typing'&&userId&&msg.chat_id) {
      if (!q.isMember.get(msg.chat_id,userId)) return;
      broadcast({type:'typing',chat_id:msg.chat_id,user_id:userId},chatMemberIds(msg.chat_id).filter(id=>id!==userId));
    }
  });
  ws.on('close', () => {
    if (!userId) return;
    const socks = conns.get(userId);
    if (socks) {
      socks.delete(ws);
      if (socks.size===0) {
        conns.delete(userId);
        const chats = q.userChats.all(userId);
        const contacts = new Set();
        chats.forEach(c => q.chatMemberIds.all(c.id).forEach(r => { if(r.user_id!==userId) contacts.add(r.user_id); }));
        broadcast({type:'user_offline',user_id:userId},[...contacts]);
        q.touchUser.run(Date.now(), userId);
      }
    }
  });
  ws.on('error', ()=>{});
});

server.listen(PORT, () => console.log(`🕊️  Голубь v5.0 на порту ${PORT}\n   DB: ${DB_PATH}`));
process.on('SIGTERM', ()=>{ db.close(); process.exit(0); });
process.on('SIGINT',  ()=>{ db.close(); process.exit(0); });
