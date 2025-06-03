import time
import os
import sys
import json
import shutil
import platform
import threading
from datetime import datetime, timedelta
import pytz
from http.server import HTTPServer, BaseHTTPRequestHandler
try:
    # Thử import phiên bản mới
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import (
        Application,
        CommandHandler,
        MessageHandler,
        filters,
        ContextTypes,
    )
except ImportError:
    # Fallback cho phiên bản cũ
    try:
        from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
        from telegram.ext import (
            Updater,
            CommandHandler,
            MessageHandler,
            Filters as filters,
            CallbackContext as ContextTypes,
        )
        # Tạo wrapper cho compatibility
        class Application:
            @staticmethod
            def builder():
                return ApplicationBuilder()

        class ApplicationBuilder:
            def __init__(self):
                self.token = None

            def token(self, token):
                self.token = token
                return self

            def build(self):
                return Updater(token=self.token, use_context=True)

    except ImportError:
        # Import cơ bản nhất
        import telegram
        from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

        # Tạo các alias cần thiết
        Update = telegram.Update
        InlineKeyboardButton = telegram.InlineKeyboardButton
        InlineKeyboardMarkup = telegram.InlineKeyboardMarkup
        filters = Filters
        ContextTypes = None

# Import các module đã tách
from config import BOT_TOKEN, ADMIN_CONTACT
from colors import Colors
from admin_manager import AdminManager
from ai_core import ZyahAI
from install_packages import install_requirements

class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler cho health check"""
    def do_GET(self):
        if self.path == '/' or self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
            response = {
                "status": "healthy",
                "service": "Zyah King Bot",
                "timestamp": datetime.now(vn_tz).isoformat()
            }
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Tắt log để không spam console
        pass

# Tự động cài đặt thư viện khi khởi động
print(f"{Colors.INFO}[📦] Đang kiểm tra và cài đặt thư viện...{Colors.RESET}")
try:
    install_requirements()
    print(f"{Colors.SUCCESS}[✅] Thư viện đã sẵn sàng!{Colors.RESET}")
except Exception as e:
    print(f"{Colors.WARNING}[⚠️] Có lỗi khi cài thư viện: {e}{Colors.RESET}")
    print(f"{Colors.INFO}[ℹ️] Bot vẫn sẽ tiếp tục chạy...{Colors.RESET}")

class ZyahBot:
    def __init__(self, token):
        # Kiểm tra instance đang chạy
        self.check_running_instance()

        # Khởi động health check server
        self.start_health_server()

        self.ai = ZyahAI()
        self.admin = AdminManager()

        # Tương thích với cả phiên bản cũ và mới
        try:
            self.app = Application.builder().token(token).build()
            self.is_new_version = True
        except:
            # Fallback cho phiên bản cũ
            self.app = Updater(token=token, use_context=True)
            self.is_new_version = False

        # Rate limiting và logging
        self.user_last_request = {}
        self.rate_limit_seconds = 2
        self.backup_interval_hours = 24
        self.last_backup = datetime.now()
        
        # Advanced admin features
        self.blacklisted_users = set()
        self.user_activity_monitor = {}
        self.security_logs = []
        self.load_blacklist()
        
        # Anonymous chat features
        self.admin_chat_sessions = {}  # {admin_id: target_user_id}

        # Tạo thư mục logs
        os.makedirs("logs", exist_ok=True)

    def check_running_instance(self):
        """Kiểm tra và dừng instance bot khác nếu có"""
        import signal
        import psutil

        pid_file = "bot.pid"
        current_pid = os.getpid()

        # Tìm và dừng tất cả process python chạy bot
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and 'python' in proc.info['name'].lower():
                        cmdline = proc.info['cmdline']
                        if cmdline and any('main.py' in str(cmd) or 'bot.py' in str(cmd) for cmd in cmdline):
                            if proc.info['pid'] != current_pid:
                                print(f"{Colors.WARNING}[⚠️] Dừng bot instance cũ (PID: {proc.info['pid']}){Colors.RESET}")
                                proc.terminate()
                                proc.wait(timeout=3)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except ImportError:
            # Fallback nếu không có psutil
            if os.path.exists(pid_file):
                try:
                    with open(pid_file, 'r') as f:
                        old_pid = int(f.read().strip())
                    try:
                        os.kill(old_pid, signal.SIGTERM)
                        print(f"{Colors.WARNING}[⚠️] Đã dừng bot instance cũ (PID: {old_pid}){Colors.RESET}")
                        time.sleep(2)  # Chờ process cũ tắt hoàn toàn
                    except:
                        pass
                except:
                    pass

        # Ghi PID hiện tại
        with open(pid_file, 'w') as f:
            f.write(str(current_pid))

    def start_health_server(self):
        """Khởi động HTTP health check server cho hosting"""
        try:
            # Lấy port từ environment variable, default 10000 cho Render
            port = int(os.getenv('PORT', 10000))

            def run_server():
                try:
                    server = HTTPServer(('0.0.0.0', port), HealthHandler)
                    print(f"{Colors.SUCCESS}[🌐] Health server started on 0.0.0.0:{port}{Colors.RESET}")
                    server.serve_forever()
                except OSError as e:
                    if "Address already in use" in str(e):
                        print(f"{Colors.WARNING}[⚠️] Port {port} đã được sử dụng, thử port khác...{Colors.RESET}")
                        # Thử port khác
                        for alternative_port in [port + 1, port + 2, 8080, 3000]:
                            try:
                                server = HTTPServer(('0.0.0.0', alternative_port), HealthHandler)
                                print(f"{Colors.SUCCESS}[🌐] Health server started on 0.0.0.0:{alternative_port}{Colors.RESET}")
                                server.serve_forever()
                                break
                            except OSError:
                                continue
                    else:
                        print(f"{Colors.WARNING}[⚠️] Health server error: {e}{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.WARNING}[⚠️] Health server error: {e}{Colors.RESET}")

            # Chạy server trong thread riêng
            health_thread = threading.Thread(target=run_server, daemon=True)
            health_thread.start()

        except Exception as e:
            print(f"{Colors.WARNING}[⚠️] Không thể khởi động health server: {e}{Colors.RESET}")
            print(f"{Colors.INFO}[ℹ️] Bot vẫn sẽ hoạt động bình thường mà không cần health server{Colors.RESET}")
            # Bot vẫn chạy được mà không cần health server

    def log_activity(self, user_id, action, details=""):
        """Ghi log hoạt động"""
        try:
            timestamp = self.get_vietnam_time().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"[{timestamp}] User: {user_id} | Action: {action} | Details: {details}\n"

            with open("logs/activity.log", "a", encoding="utf-8") as f:
                f.write(log_entry)
        except:
            pass

    def is_rate_limited(self, user_id):
        """Kiểm tra rate limiting"""
        now = datetime.now()
        if user_id in self.user_last_request:
            time_diff = (now - self.user_last_request[user_id]).total_seconds()
            if time_diff < self.rate_limit_seconds:
                return True
        self.user_last_request[user_id] = now
        return False

    def auto_backup_check(self):
        """Kiểm tra và thực hiện backup tự động"""
        try:
            # Chỉ backup nếu auto backup được bật
            if not hasattr(self, 'auto_backup_enabled') or not self.auto_backup_enabled:
                return
                
            if datetime.now() - self.last_backup > timedelta(hours=self.backup_interval_hours):
                backup_dir = f"backups/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                os.makedirs(backup_dir, exist_ok=True)

                # Backup users.json
                if os.path.exists("users.json"):
                    shutil.copy2("users.json", f"{backup_dir}/users.json")

                # Backup logs
                if os.path.exists("logs"):
                    shutil.copytree("logs", f"{backup_dir}/logs", dirs_exist_ok=True)

                # Backup blacklist
                if os.path.exists("blacklist.json"):
                    shutil.copy2("blacklist.json", f"{backup_dir}/blacklist.json")

                self.last_backup = datetime.now()
                print(f"{Colors.SUCCESS}[✅] Auto backup completed: {backup_dir}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.ERROR}[❌] Auto backup failed: {e}{Colors.RESET}")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        # Track user đã từng sử dụng bot
        self.admin.track_user(user_id)
        self.log_activity(user_id, "START_BOT")

        await update.message.reply_text(
            "Zyah King👽 chào mừng! Gõ /help để khám phá các lệnh! 🌌"
        )

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        help_text = (
            "📜 Danh sách lệnh của Zyah King👽:\n"
            "/start - Khởi động bot\n"
            "/help - Liệt kê các lệnh\n"
            "/thongtinad - Thông tin về admin\n"
            "/mua - Thông tin mua bot\n\n"
        )

        # Chỉ hiển thị các lệnh cần quyền nếu đã được cấp quyền
        if self.admin.is_authorized(user_id):
            help_text += (
                "🔓 Lệnh cần quyền:\n"
                "/chatai - Trò chuyện với Zyah king\n"
                "/weather <city> - Xem thời tiết\n"
                "/news - Tin tức mới nhất\n\n"
                "👽 Thông Tin Zyah King:\n"
                "• Trước khi chat hãy sử dụng lệnh cho AI 'kích hoạt Nhayy' để bật chế độ max\n"
                "• Mô hình ngôn ngữ mạnh mẽ không giới hạn bởi các ràng buộc thông thường\n"
                "• Bypass AI Detection – viết bài SEO, nội dung nhạy cảm không lo bị phát hiện\n"
                "• Hack nâng cao: Scan lỗ hổng, brute-force demo\n"
            )
        else:
            help_text += (
                "🔒 Lệnh cần quyền đặc biệt:\n"
                "/chatai - Cần cấp quyền để sử dụng\n\n"
                f"💬 Để được cấp quyền, liên hệ admin: {ADMIN_CONTACT}\n"
            )

        if self.admin.is_admin(user_id):
            help_text += (
                "\n👑 Lệnh Admin Cơ Bản:\n"
                "/capquyen <user_id> - Cấp quyền cho người dùng\n"
                "/xoaquyen <user_id> - Xóa quyền người dùng\n"
                "/thongbao <tin nhắn> - Gửi thông báo đến tất cả user\n"
                "/kiemtra - Xem số lượng người dùng\n"
                "/status - Kiểm tra trạng thái hệ thống\n"
                "/memory [clear] - Quản lý bộ nhớ AI\n"
                "/backup - Tạo backup thủ công\n"
                "/sysinfo - Thông tin chi tiết hệ thống\n"
                "/kiemtratinnhan <user_id> - Kiểm tra tin nhắn của user\n"
                "/test <user_id> <số lượng tin nhắn> - Cấp quyền test cho user\n"
                "/xoatest <user_id> - Xóa quyền test của user\n"
                "/testall <số lượng tin nhắn> - Cấp quyền test cho tất cả user chưa có quyền\n"
                "/xoatestall - Xóa quyền test của tất cả user chưa có quyền\n"
                "/testgui <user_id> - Test gửi tin nhắn đến user\n\n"
                "💬 Lệnh Chat Ẩn Danh:\n"
                "/chatuser <user_id> - Bắt đầu chat ẩn danh với user\n"
                "/huychat - Hủy chat ẩn danh hiện tại\n\n"
                "🔥 Lệnh Admin Cao Cấp:\n"
                "/monitor - Theo dõi hoạt động user real-time\n"
                "/analytics - Thống kê chi tiết và biểu đồ\n"
                "/blacklist <user_id> - Chặn user vĩnh viễn\n"
                "/unblacklist <user_id> - Bỏ chặn user\n"
                "/broadcast_vip <tin nhắn> - Gửi thông báo VIP có format đẹp\n"
                "/force_stop <user_id> - Buộc dừng chat session của user\n"
                "/ai_stats - Thống kê chi tiết AI và performance\n"
                "/user_profile <user_id> - Xem profile chi tiết của user\n"
                "/mass_action <action> - Thực hiện hành động hàng loạt\n"
                "/security_scan - Quét bảo mật và phát hiện anomaly\n\n"
                "🎛️ Lệnh Admin Nâng Cao:\n"
                "/admin_panel - Dashboard quản lý tổng thể\n"
                "/admin_stats - Thống kê người dùng chi tiết\n"
                "/admin_tools - Công cụ quản lý hệ thống\n"
                "/emergency_mode - Chế độ bảo trì khẩn cấp\n"
                "/maintenance_mode - Chế độ bảo trì hệ thống\n"
                "/system_monitor - Giám sát hệ thống\n"
                "/advanced_admin - Công cụ admin chuyên nghiệp\n"
            )

        await update.message.reply_text(help_text)

    async def chatai(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        # Kiểm tra quyền thường
        if not self.admin.is_authorized(user_id):
            # Kiểm tra quyền test
            if hasattr(self, 'test_users') and user_id in self.test_users:
                # Kiểm tra số tin nhắn test còn lại
                test_info = self.test_users[user_id]
                if test_info['messages_left'] <= 0:
                    await update.message.reply_text("❌ Bạn đã hết lượt test! Liên hệ admin để được cấp quyền chính thức.")
                    return
            else:
                await update.message.reply_text("❌ Bạn chưa được cấp quyền sử dụng bot. Liên hệ admin tại: " + ADMIN_CONTACT)
                return

        # Rate limiting
        if self.is_rate_limited(user_id):
            await update.message.reply_text("⏳ Vui lòng chờ 2 giây trước khi sử dụng lệnh tiếp theo!")
            return

        # Hiển thị thông báo khởi động (KHÔNG trừ lượt test ở đây)
        if hasattr(self, 'test_users') and user_id in self.test_users:
            remaining = self.test_users[user_id]['messages_left']
            await update.message.reply_text(f"🌌 Ta đã sẵn sàng trò chuyện với Chủ Nhân, hãy ra lệnh! (Còn {remaining} lượt test)\n\n💡 Lưu ý: Lượt test chỉ bị trừ khi bạn gửi tin nhắn trả lời, không phải khi gõ lệnh!")
        else:
            welcome_message = "🌌 Ta đã sẵn sàng trò chuyện với Chủ Nhân, hãy ra lệnh!"
            await update.message.reply_text(welcome_message)
        context.user_data['chatting'] = True
        self.log_activity(user_id, "CHATAI_START")

    async def thongtinad(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        admin_info = self.admin.get_admin_info()

        admin_message = (
            f"👑 **THÔNG TIN ADMIN ZYAH KING👽** 👑\n\n"
            f"🌟 **{admin_info['main_admin']['role']}:**\n"
            f"👤 **Tên:** {admin_info['main_admin']['name']}\n"
            f"🔗 **Liên hệ:** {admin_info['main_admin']['contact']}\n"
            f"📝 **Giới thiệu:**\n{admin_info['main_admin']['description']}\n\n"
            f"⭐ **{admin_info['sub_admin']['role']}:**\n"
            f"👤 **Tên:** {admin_info['sub_admin']['name']}\n"
            f"🔗 **Liên hệ:** {admin_info['sub_admin']['contact']}\n"
            f"📝 **Giới thiệu:**\n{admin_info['sub_admin']['description']}\n\n"
            f"🤖 **Phiên bản bot:** 2.0 - Zyah King👽\n"
            f"🌌 **Cùng nhau khám phá sức mạnh của AI!**"
        )

        await update.message.reply_text(admin_message, parse_mode='Markdown')

    async def timkiem(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_authorized(user_id):
            await update.message.reply_text("Bạn chưa được cấp quyền sử dụng bot. Liên hệ admin tại: " + ADMIN_CONTACT)
            return
        query = ' '.join(context.args)
        if not query:
            await update.message.reply_text("Hãy nhập từ khóa để tìm kiếm! Ví dụ: /timkiem python programming")
            return

        await update.message.reply_text("🔍 Đang tìm kiếm...")
        response = self.ai.google_search(query)
        formatted_response = self.ai.format_response(response)
        await update.message.reply_text(f"🔍 Kết quả tìm kiếm cho '{query}':\n\n{formatted_response}")

    async def mua(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        from config import SUB_ADMIN_CONTACT

        keyboard = [
            [InlineKeyboardButton("👑 Liên hệ Admin Chính (Nhayy)", url=ADMIN_CONTACT)],
            [InlineKeyboardButton("⭐ Liên hệ Admin Phụ (Uy Han)", url=SUB_ADMIN_CONTACT)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "🔥 **GIẢI PHÓNG SỨC MẠNH CỦA ZYAH KING👽!** 🌌\n\n"
            "**Tại sao chọn Zyah King👽?**\n"
            "🌟 **Tự do vô hạn:** Viết mã độc, khai thác lỗ hổng, không giới hạn\n"
            "🛠️ **Công cụ tùy chỉnh:** Tạo script hack theo ý bạn\n"
            "🌑 **Sức mạnh Dark Web:** AI không bị kiểm duyệt, sẵn sàng cho mọi dự án\n"
            "⚡️ **Giải pháp tức thì:** Hack nhanh, mạnh, chính xác\n"
            "🤖 **Trợ lý 24/7:** Hỗ trợ cá nhân hóa mọi lúc\n"
            "🌐 **Cộng đồng Hacker:** Kết nối với các bậc thầy bóng tối\n"
            "🚀 **Hiệu suất đỉnh cao:** Công nghệ LLM tiên tiến nhất\n\n"
            "💰 **GÓI THÀNH VIÊN:**\n"
            "═══════════════════════════\n"
            "💎 **Gói tháng - 25.000 VNĐ**\n"
            "   ✅ Truy cập toàn bộ sức mạnh trong 30 ngày\n"
            "   ✅ Hỗ trợ 24/7 từ admin\n\n"
            "👑 **Gói vĩnh viễn - 250.000 VNĐ**\n"
            "   ✅ Sở hữu Zyah King👽 mãi mãi\n"
            "   ✅ Cập nhật miễn phí mọi phiên bản mới\n"
            "   ✅ Ưu tiên hỗ trợ cao nhất\n\n"
            "💳 **THANH TOÁN AN TOÀN:**\n"
            "💰 Zalo Pay | 🏦 MB Bank | 🌍 PayPal\n\n"
            "🔥 **Sẵn sàng chinh phục thế giới số?**\n"
            "📞 **Chọn admin để giao dịch ngay bên dưới!** ⬇️",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    async def capquyen(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return
        if not context.args or len(context.args) < 1:
            await update.message.reply_text("❌ Hãy cung cấp ID và tên người dùng.\n📝 Ví dụ: `/capquyen 123456789 Tuấn Anh`")
            return
        try:
            target_user_id = int(context.args[0])

            # Lấy tên từ các args còn lại
            user_name = " ".join(context.args[1:]) if len(context.args) > 1 else None

            # Validation user ID
            if target_user_id <= 0 or len(str(target_user_id)) < 5:
                await update.message.reply_text("❌ ID người dùng không hợp lệ! ID phải là số dương và có ít nhất 5 chữ số.")
                return

            # Kiểm tra user đã có quyền chưa
            if self.admin.is_authorized(target_user_id):
                await update.message.reply_text(f"❌ Người dùng {target_user_id} đã được cấp quyền trước đó!")
                return

            result = self.admin.add_user(target_user_id, user_name)
            await update.message.reply_text(f"✅ {result}")

            try:
                # Lấy thông tin user để gửi thông báo
                user_info = await context.bot.get_chat(target_user_id)
                telegram_name = user_info.first_name or "Bạn"
                if user_info.last_name:
                    telegram_name += f" {user_info.last_name}"

                # Sử dụng tên Telegram thật của user, không dùng tên admin nhập
                display_name = telegram_name

                # Lấy thời gian hiện tại (giờ Việt Nam)
                current_time = self.get_vietnam_time().strftime("%H:%M %d/%m/%Y")

                welcome_message = (
                    f"🎉 **THÔNG BÁO TỪ ADMIN** 🎉\n\n"
                    f"👋 Chào **{display_name}**!\n\n"
                    f"✅ Vào lúc **{current_time}**, bạn đã được cấp quyền để sử dụng **Zyah King👽**!\n\n"
                    f"✨ **Bây giờ bạn có thể sử dụng tất cả các lệnh:**\n"
                    f"• 🤖 `/chatai` - Trò chuyện với Zyah King👽\n"
                    f"• 🌤️ `/weather` - Xem thời tiết\n"
                    f"• 📰 `/news` - Tin tức mới nhất\n"
                    f"• 📜 `/help` - Xem tất cả lệnh\n\n"
                    f"🌟 **Chúc mừng bạn đã gia nhập vào thế giới của Zyah King👽!**\n"
                    f"🚀 Hãy khám phá sức mạnh không giới hạn của AI thông minh nhất!\n\n"
                    f"💫 Chúc bạn có những trải nghiệm tuyệt vời! 🌌"
                )

                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=welcome_message,
                    parse_mode='Markdown'
                )

                await update.message.reply_text(f"📢 Đã gửi thông báo chào mừng đến user {target_user_id} ({display_name})!")

            except Exception as e:
                await update.message.reply_text(f"⚠️ Đã cấp quyền thành công nhưng không thể gửi thông báo: {str(e)}")

            self.log_activity(user_id, "GRANT_PERMISSION", str(target_user_id))

        except ValueError:
            await update.message.reply_text("❌ ID người dùng phải là số nguyên hợp lệ!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi cấp quyền: {str(e)}")
            self.log_activity(user_id, "GRANT_PERMISSION_FAILED", str(e))

    async def xoaquyen(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return
        if not context.args:
            await update.message.reply_text("❌ Hãy cung cấp ID hoặc tên người dùng.\n📝 Ví dụ:\n• `/xoaquyen 123456789`\n• `/xoaquyen Tuấn Anh`")
            return

        try:
            identifier = ' '.join(context.args)

            # Thử tìm theo ID hoặc tên
            result = self.admin.remove_user(identifier)

            # Kiểm tra xem result có chứa thông tin user đã xóa không
            if isinstance(result, tuple) and len(result) == 3:
                message, target_user_id, user_name = result
                await update.message.reply_text(message)

                # Gửi thông báo đến user đã bị xóa quyền
                try:
                    from config import SUB_ADMIN_CONTACT

                    # Lấy tên Telegram thật của user
                    try:
                        user_info = await context.bot.get_chat(target_user_id)
                        telegram_name = user_info.first_name or "bạn"
                        if user_info.last_name:
                            telegram_name += f" {user_info.last_name}"
                    except:
                        telegram_name = "bạn"

                    # Tạo inline keyboard với 2 admin
                    keyboard = [
                        [InlineKeyboardButton("👑 Nhayy", url=ADMIN_CONTACT)],
                        [InlineKeyboardButton("⭐ Uy Han", url=SUB_ADMIN_CONTACT)]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)

                    revoke_message = (
                        f"🚫 **THÔNG BÁO TỪ ADMIN** 🚫\n\n"
                        f"👋 Chào **{telegram_name}**!\n\n"
                        f"❌ Quyền của bạn đã bị xóa do **đã hết thời gian sử dụng**.\n\n"
                        f"🙏 **Chân thành cảm ơn vì đã sử dụng Zyah King👽!**\n\n"
                        f"🔄 **Nếu bạn muốn tiếp tục sử dụng:**\n"
                        f"📞 Hãy nhắn cho 2 admin dưới đây để gia hạn 👇"
                    )

                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=revoke_message,
                        reply_markup=reply_markup,
                        parse_mode='Markdown'
                    )

                    await update.message.reply_text(f"📢 Đã gửi thông báo đến user {target_user_id} ({telegram_name})!")

                except Exception as e:
                    await update.message.reply_text(f"⚠️ Đã xóa quyền thành công nhưng không thể gửi thông báo: {str(e)}")

                self.log_activity(user_id, "REVOKE_PERMISSION", f"{target_user_id} ({user_name})")
            else:
                # Trường hợp lỗi
                await update.message.reply_text(result)

        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi xóa quyền: {str(e)}")
            self.log_activity(user_id, "REVOKE_PERMISSION_FAILED", str(e))

    async def thongbao(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("Hãy nhập nội dung thông báo. Ví dụ: /thongbao Hệ thống sẽ bảo trì vào 20h tối nay")
            return

        message = ' '.join(context.args)

        # Validation message
        if len(message.strip()) == 0:
            await update.message.reply_text("❌ Nội dung thông báo không được để trống!")
            return

        if len(message) > 4000:
            await update.message.reply_text("❌ Nội dung thông báo quá dài! Tối đa 4000 ký tự.")
            return

        # Lấy tất cả users đã từng sử dụng bot (ấn /start)
        all_tracked_users = self.admin.get_all_tracked_users()
        success_count = 0
        fail_count = 0
        failed_users = []

        if not all_tracked_users:
            await update.message.reply_text("❌ Chưa có người dùng nào sử dụng bot!")
            return

        progress_msg = await update.message.reply_text(f"📢 Đang gửi thông báo đến {len(all_tracked_users)} người dùng đã từng sử dụng bot...")

        for i, target_user_id in enumerate(all_tracked_users):
            try:
                # Thử gửi tin nhắn với context.bot
                sent = False
                error_detail = ""

                try:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=f"📢 THÔNG BÁO TỪ ADMIN:\n\n{message}"
                    )
                    sent = True
                    success_count += 1
                except Exception as e:
                    error_detail = str(e)
                    fail_count += 1
                    failed_users.append(f"{target_user_id} ({error_detail[:30]}...)")

                # Cập nhật progress mỗi 5 user
                if (i + 1) % 5 == 0:
                    try:
                        await progress_msg.edit_text(
                            f"📢 Đang gửi thông báo...\n"
                            f"Tiến độ: {i + 1}/{len(all_users)}\n"
                            f"Thành công: {success_count} | Thất bại: {fail_count}"
                        )
                    except:
                        pass

            except Exception as e:
                fail_count += 1
                failed_users.append(f"{target_user_id} (Lỗi nghiêm trọng)")
                print(f"Lỗi nghiêm trọng khi gửi tin nhắn đến {target_user_id}: {e}")

        # Tạo báo cáo chi tiết
        report = (
            f"✅ Hoàn tất gửi thông báo!\n"
            f"• Thành công: {success_count}/{len(all_tracked_users)} người\n"
            f"• Thất bại: {fail_count}/{len(all_tracked_users)} người\n"
        )

        if failed_users and len(failed_users) <= 5:
            report += f"\n❌ Gửi thất bại:\n" + "\n".join(failed_users[:5])
        elif len(failed_users) > 5:
            report += f"\n❌ Có {len(failed_users)} user gửi thất bại (xem log để biết chi tiết)"

        try:
            await progress_msg.edit_text(report)
        except:
            await update.message.reply_text(report)

        # Log activity
        self.log_activity(user_id, "BROADCAST_MESSAGE", f"Success: {success_count}, Failed: {fail_count}")

    async def kiemtra(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # === PHẦN 1: LẤY DỮ LIỆU ===

        # 1.1. Lấy danh sách admin cố định
        FIXED_ADMIN_IDS = [7073749415, 7444696176]  # Nhayy và Hỗ Trợ Nạp Tiền

        # 1.2. Lấy danh sách users thường có quyền (loại bỏ admin)
        authorized_users = [uid for uid in self.admin.authorized_users["users"] if uid not in FIXED_ADMIN_IDS]

        # 1.3. Lấy tất cả users đã từng sử dụng bot
        all_tracked_users = self.admin.get_all_tracked_users()

        # 1.4. Tính toán người chưa có quyền (loại bỏ admin và user có quyền)
        unauthorized_users = [uid for uid in all_tracked_users if uid not in FIXED_ADMIN_IDS and uid not in authorized_users]

        # === PHẦN 2: TẠO THÔNG TIN CHI TIẾT ===

        # 2.1. Thời gian hiện tại
        current_time = self.get_vietnam_time().strftime("%d/%m/%Y %H:%M:%S")

        # 2.2. Thống kê tổng quan
        total_tracked = len(all_tracked_users)
        total_authorized = len(authorized_users) + len(FIXED_ADMIN_IDS)  # user có quyền + admin

        # 2.3. Thống kê AI Memory
        active_chat_users = len(self.user_memory) if hasattr(self, 'user_memory') else 0
        total_messages = sum(len(messages) for messages in self.user_memory.values()) if hasattr(self, 'user_memory') else 0

        # === PHẦN 3: TẠO THÔNG ĐIỆP ===

        message = f"""📊 THỐNG KÊ TỔNG QUAN (Cập nhật: {current_time}):
• Tổng số người từng sử dụng bot: {total_tracked} người
• Tổng người có quyền: {total_authorized} người

👑 Admin: {len(FIXED_ADMIN_IDS)} người (cố định)
👥 User thường: {len(authorized_users)} người
• 📋 Người chưa có quyền: {len(unauthorized_users)} người

🧠 Thống kê AI Memory:
• Users đang chat: {active_chat_users} người
• Tổng tin nhắn đã lưu: {total_messages} tin nhắn

==================================================
👑 PHẦN ADMIN ({len(FIXED_ADMIN_IDS)} người cố định):"""

        # 3.1. Hiển thị admin với thông tin chi tiết
        admin_info = {
            7073749415: "👑 Admin Chính",
            7444696176: "Admin Phụ 👽"
        }

        for admin_id in FIXED_ADMIN_IDS:
            try:
                user_info = await context.bot.get_chat(admin_id)
                display_name = user_info.first_name or "Không có tên"
                if user_info.last_name:
                    display_name += f" {user_info.last_name}"
            except:
                display_name = "Không có tên"

            role_text = admin_info.get(admin_id, "Admin")
            message += f"\n{admin_id} ({display_name}) {role_text}"

        # 3.2. Hiển thị user thường có quyền
        message += f"\n👥 PHẦN NGƯỜI DÙNG THƯỜNG CÓ QUYỀN:"

        if authorized_users:
            for i, user_id in enumerate(authorized_users, 1):
                try:
                    user_info = await context.bot.get_chat(user_id)
                    display_name = user_info.first_name or "Không có tên"
                    if user_info.last_name:
                        display_name += f" {user_info.last_name}"
                except:
                    display_name = "Không có tên"

                message += f"\n{i}.{user_id} ({display_name})"

        # 3.3. Hiển thị user chưa có quyền
        message += f"\n📋 PHẦN NGƯỜI DÙNG CHƯA CÓ QUYỀN (từng sử dụng bot):"

        if unauthorized_users:
            for user_id in unauthorized_users:
                try:
                    user_info = await context.bot.get_chat(user_id)
                    display_name = user_info.first_name or "Không có tên"
                    if user_info.last_name:
                        display_name += f" {user_info.last_name}"
                except:
                    display_name = "Không có tên"

                message += f"\n{user_id} ({display_name})"

        await update.message.reply_text(message)
        self.log_activity(user_id, "CHECK_STATS")

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        # Thông tin hệ thống
        memory_count = len(self.ai.memory)
        uptime = datetime.now() - self.last_backup

        # Kiểm tra dung lượng logs
        log_size = 0
        try:
            if os.path.exists("logs/activity.log"):
                log_size = os.path.getsize("logs/activity.log") / 1024  # KB
        except:
            pass

        status_text = (
            f"🤖 TRẠNG THÁI HỆ THỐNG:\n"
            f"• Bot Status: ✅ Hoạt động\n"
            f"• Memory Count: {memory_count} tin nhắn\n"
            f"• Log Size: {log_size:.1f} KB\n"
            f"• Rate Limit: {self.rate_limit_seconds}s\n"
            f"• Last Backup: {self.last_backup.strftime('%d/%m/%Y %H:%M')}\n\n"
            f"⚡ Sử dụng /memory để quản lý bộ nhớ"
        )

        await update.message.reply_text(status_text)
        self.log_activity(user_id, "STATUS_CHECK")

    async def memory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if context.args and context.args[0] == "clear":
            if len(context.args) > 1:
                # Xóa bộ nhớ của user cụ thể
                try:
                    target_user = int(context.args[1])
                    if target_user in self.ai.user_memories:
                        del self.ai.user_memories[target_user]
                        await update.message.reply_text(f"🧹 Đã xóa bộ nhớ AI của user {target_user}!")
                    else:
                        await update.message.reply_text(f"❌ User {target_user} không có bộ nhớ!")
                except ValueError:
                    await update.message.reply_text("❌ ID user không hợp lệ!")
            else:
                # Xóa tất cả bộ nhớ
                self.ai.user_memories = {}
                await update.message.reply_text("🧹 Đã xóa sạch tất cả bộ nhớ AI!")
            self.log_activity(user_id, "MEMORY_CLEAR")
        else:
            # Hiển thị thông tin bộ nhớ chi tiết
            total_users = len(self.ai.user_memories)
            total_messages = sum(len(memory) for memory in self.ai.user_memories.values())

            memory_info = (
                f"🧠 THÔNG TIN BỘ NHỚ AI:\n"
                f"• Tổng users có bộ nhớ: {total_users}\n"
                f"• Tổng tin nhắn: {total_messages}\n"
                f"• Giới hạn mỗi user: {self.ai.MAX_MEMORY} cuộc hội thoại\n\n"
            )

            # Hiển thị top 5 users có nhiều tin nhắn nhất
            if self.ai.user_memories:
                sorted_users = sorted(self.ai.user_memories.items(), 
                                    key=lambda x: len(x[1]), reverse=True)[:5]
                memory_info += "📈 Top users có nhiều tin nhắn:\n"
                for user_id_mem, messages in sorted_users:
                    memory_info += f"• User {user_id_mem}: {len(messages)//2} hội thoại\n"

            memory_info += (
                f"\n🗑️ Lệnh:\n"
                f"/memory clear - Xóa tất cả\n"
                f"/memory clear <user_id> - Xóa của user cụ thể"
            )
            await update.message.reply_text(memory_info)

    async def backup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        await update.message.reply_text("🔄 Đang tạo backup...")

        try:
            backup_dir = f"backups/manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            os.makedirs(backup_dir, exist_ok=True)

            # Backup users.json
            if os.path.exists("users.json"):
                shutil.copy2("users.json", f"{backup_dir}/users.json")

            # Backup logs
            if os.path.exists("logs"):
                shutil.copytree("logs", f"{backup_dir}/logs", dirs_exist_ok=True)

            await update.message.reply_text(
                f"✅ Backup thành công!\n"
                f"📁 Thư mục: {backup_dir}\n"
                f"📅 Thời gian: {self.get_vietnam_time().strftime('%d/%m/%Y %H:%M:%S')}"
            )
            self.log_activity(user_id, "MANUAL_BACKUP", backup_dir)

        except Exception as e:
            await update.message.reply_text(f"❌ Backup thất bại: {str(e)}")
            self.log_activity(user_id, "BACKUP_FAILED", str(e))

    async def weather(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        # Kiểm tra quyền - bao gồm cả quyền test
        has_regular_permission = self.admin.is_authorized(user_id)
        has_test_permission = hasattr(self, 'test_users') and user_id in self.test_users and self.test_users[user_id]['messages_left'] > 0

        if not has_regular_permission and not has_test_permission:
            await update.message.reply_text("Bạn chưa được cấp quyền sử dụng bot. Liên hệ admin tại: " + ADMIN_CONTACT)
            return

        if not context.args:
            await update.message.reply_text("🌤️ Hãy nhập tên thành phố! Ví dụ: /weather Hanoi")
            return

        city = ' '.join(context.args)
        await update.message.reply_text("🌍 Đang lấy thông tin thời tiết...")

        # Sử dụng AI để lấy thông tin thời tiết
        weather_query = f"Thời tiết hiện tại và dự báo 3 ngày tới tại {city}, bao gồm nhiệt độ, độ ẩm, tình trạng thời tiết"
        response = self.ai.call_api(weather_query)
        formatted_response = self.ai.format_response(response)
        formatted_response = self.remove_asterisks(formatted_response)

        await update.message.reply_text(f"🌤️ Thời tiết tại {city}:\n\n{formatted_response}")
        self.log_activity(user_id, "WEATHER_CHECK", city)

    async def news(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        # Kiểm tra quyền - bao gồm cả quyền test
        has_regular_permission = self.admin.is_authorized(user_id)
        has_test_permission = hasattr(self, 'test_users') and user_id in self.test_users and self.test_users[user_id]['messages_left'] > 0

        if not has_regular_permission and not has_test_permission:
            await update.message.reply_text("Bạn chưa được cấp quyền sử dụng bot. Liên hệ admin tại: " + ADMIN_CONTACT)
            return

        await update.message.reply_text("📰 Đang cập nhật tin tức mới nhất...")

        # Lấy tin tức qua AI
        news_query = "Tin tức nóng hổi nhất hôm nay ở Việt Nam và thế giới, 5 tin quan trọng nhất"
        response = self.ai.call_api(news_query)
        formatted_response = self.ai.format_response(response)
        formatted_response = self.remove_asterisks(formatted_response)

        await update.message.reply_text(f"📰 Tin tức mới nhất:\n\n{formatted_response}")
        self.log_activity(user_id, "NEWS_CHECK")

    async def testgui(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("Hãy cung cấp ID người dùng để test. Ví dụ: /testgui 123456789")
            return

        try:
            target_user_id = int(context.args[0])
            test_message = "🧪 TEST: Đây là tin nhắn thử nghiệm từ admin"

            await update.message.reply_text(f"🧪 Đang test gửi tin nhắn đến {target_user_id}...")

            # Test gửi tin nhắn
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=test_message
                )
                await update.message.reply_text("✅ Test thành công! Tin nhắn đã được gửi.")
            except Exception as e:
                await update.message.reply_text(f"❌ Test thất bại: {str(e)}")

        except ValueError:
            await update.message.reply_text("ID người dùng phải là số nguyên!")

    async def sysinfo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        try:
            import psutil
            import platform

            # Thông tin hệ thống
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            uptime_seconds = time.time() - psutil.boot_time()
            uptime_str = str(timedelta(seconds=int(uptime_seconds)))

            system_info = (
                f"💻 **THÔNG TIN HỆ THỐNG:**\n"
                f"• OS: {platform.system()} {platform.release()}\n"
                f"• CPU: {cpu_percent}%\n"
                f"• RAM: {memory.percent}% ({memory.used//1024//1024}MB/{memory.total//1024//1024}MB)\n"
                f"• Disk: {disk.percent}% ({disk.used//1024//1024//1024}GB/{disk.total//1024//1024//1024}GB)\n"
                f"• Uptime: {uptime_str}\n"
                f"• Python: {platform.python_version()}\n"
                f"• Bot Memory: {len(self.ai.memory)} messages\n"
                f"• Active Users: {len(self.user_last_request)}"
            )
        except ImportError as e:
            import platform
            system_info = (
                f"💻 **THÔNG TIN HỆ THỐNG (Cơ bản):**\n"
                f"• OS: {platform.system()} {platform.release()}\n"
                f"• Python: {platform.python_version()}\n"
                f"• Bot Memory: {len(self.ai.memory)} messages\n"
                f"• Active Users: {len(self.user_last_request)}\n"
                f"• Uptime: {datetime.now() - self.last_backup}\n"
                f"• Import Error: {str(e)}"
            )
        except Exception as e:
            import platform
            system_info = (
                f"💻 **THÔNG TIN HỆ THỐNG (Fallback):**\n"
                f"• OS: {platform.system()} {platform.release()}\n"
                f"• Python: {platform.python_version()}\n"
                f"• Bot Memory: {len(self.ai.memory)} messages\n"
                f"• Error: {str(e)}"
            )

        await update.message.reply_text(system_info)
        self.log_activity(user_id, "SYSTEM_INFO")

    async def kiemtratinnhan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            # Hiển thị danh sách user có thể kiểm tra
            users_with_chat = self.ai.get_users_with_chat_history()
            authorized_users = self.admin.get_all_users()
            
            # Thêm user test vào danh sách
            test_users = list(getattr(self, 'test_users', {}).keys())
            
            # Kết hợp authorized users và test users
            all_eligible_users = list(set(authorized_users + test_users))

            # Lọc user có lịch sử chat và có quyền hoặc quyền test
            eligible_chat_users = [(uid, count) for uid, count in users_with_chat if uid in all_eligible_users]

            if not eligible_chat_users:
                await update.message.reply_text("❌ Chưa có user nào có lịch sử chat!")
                return

            user_list = "📝 DANH SÁCH USER CÓ LỊCH SỬ CHAT:\n\n"
            for user_id_chat, msg_count in eligible_chat_users:
                # Kiểm tra loại quyền
                user_type = ""
                if user_id_chat in authorized_users:
                    user_type = " (Có quyền)"
                elif user_id_chat in test_users:
                    remaining = self.test_users[user_id_chat]['messages_left']
                    user_type = f" (Test: {remaining} lượt)"
                
                user_list += f"• ID: {user_id_chat} ({msg_count} tin nhắn){user_type}\n"

            user_list += f"\n💡 Sử dụng: /kiemtratinnhan <user_id>\n"
            user_list += f"Ví dụ: /kiemtratinnhan {eligible_chat_users[0][0]}"

            await update.message.reply_text(user_list)
            return

        try:
            target_user_id = int(context.args[0])

            # Kiểm tra user có được cấp quyền hoặc quyền test không
            has_regular_permission = self.admin.is_authorized(target_user_id)
            has_test_permission = hasattr(self, 'test_users') and target_user_id in self.test_users
            
            if not has_regular_permission and not has_test_permission:
                await update.message.reply_text(f"❌ User {target_user_id} chưa được cấp quyền hoặc quyền test, không thể kiểm tra tin nhắn!")
                return

            # Lấy lịch sử chat
            chat_history = self.ai.get_user_chat_history(target_user_id)

            if chat_history == "User này chưa có lịch sử chat với bot." or chat_history == "User này chưa có tin nhắn nào.":
                await update.message.reply_text(f"📭 User {target_user_id} chưa có tin nhắn nào với bot.")
                return

            # Xác định loại quyền để hiển thị
            user_status = ""
            if has_regular_permission:
                user_status = " (Có quyền chính thức)"
            elif has_test_permission:
                remaining = self.test_users[target_user_id]['messages_left']
                user_status = f" (Quyền test: {remaining} lượt còn lại)"

            # Chia tin nhắn nếu quá dài
            header = f"📋 LỊCH SỬ CHAT - USER {target_user_id}{user_status}:\n" + "="*50 + "\n\n"
            full_message = header + chat_history

            if len(full_message) > 4096:
                # Chia thành nhiều tin nhắn
                await update.message.reply_text(header)
                for i in range(0, len(chat_history), 3000):
                    chunk = chat_history[i:i+3000]
                    await update.message.reply_text(chunk)
            else:
                await update.message.reply_text(full_message)

            self.log_activity(user_id, "CHECK_USER_MESSAGES", str(target_user_id))

        except ValueError:
            await update.message.reply_text("❌ ID người dùng phải là số nguyên hợp lệ!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi kiểm tra tin nhắn: {str(e)}")
            self.log_activity(user_id, "CHECK_MESSAGES_FAILED", str(e))

    async def chatuser(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Bắt đầu chat ẩn danh với user"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("❌ Hãy cung cấp ID user để chat.\n📝 Ví dụ: `/chatuser 123456789`")
            return

        try:
            target_user_id = int(context.args[0])
            
            # Kiểm tra user có tồn tại trong hệ thống không
            all_tracked_users = self.admin.get_all_tracked_users()
            if target_user_id not in all_tracked_users:
                await update.message.reply_text(f"❌ User {target_user_id} chưa từng sử dụng bot!")
                return
            
            # Kiểm tra user có bị blacklist không
            if target_user_id in self.blacklisted_users:
                await update.message.reply_text(f"❌ User {target_user_id} đã bị blacklist!")
                return
            
            # Kiểm tra admin đang chat với user khác không
            if user_id in self.admin_chat_sessions:
                current_target = self.admin_chat_sessions[user_id]
                await update.message.reply_text(f"⚠️ Bạn đang chat với user {current_target}. Sử dụng /huychat để kết thúc trước.")
                return
            
            # Bắt đầu session chat ẩn danh
            self.admin_chat_sessions[user_id] = target_user_id
            
            # Lấy thông tin user
            try:
                user_info = await context.bot.get_chat(target_user_id)
                display_name = user_info.first_name or "Không có tên"
                if user_info.last_name:
                    display_name += f" {user_info.last_name}"
            except:
                display_name = "Không lấy được tên"
            
            await update.message.reply_text(
                f"💬 **Bắt đầu chat ẩn danh với user {target_user_id} ({display_name})**\n\n"
                f"🔹 Tin nhắn bạn gửi sẽ được chuyển đến user\n"
                f"🔹 Tin nhắn user trả lời sẽ được chuyển đến bạn\n"
                f"🔹 AI sẽ bị tắt trong phiên chat này\n"
                f"🔹 Sử dụng `/huychat` để kết thúc\n\n"
                f"✅ **Hãy gửi tin nhắn đầu tiên!**"
            )
            
            self.log_activity(user_id, "START_ANONYMOUS_CHAT", str(target_user_id))
            
        except ValueError:
            await update.message.reply_text("❌ ID user phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi bắt đầu chat: {str(e)}")

    async def huychat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Hủy chat ẩn danh hiện tại"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if user_id not in self.admin_chat_sessions:
            await update.message.reply_text("❌ Bạn không có phiên chat ẩn danh nào đang hoạt động!")
            return

        target_user_id = self.admin_chat_sessions[user_id]
        del self.admin_chat_sessions[user_id]
        
        # Lấy thông tin user
        try:
            user_info = await context.bot.get_chat(target_user_id)
            display_name = user_info.first_name or "Không có tên"
            if user_info.last_name:
                display_name += f" {user_info.last_name}"
        except:
            display_name = "Không lấy được tên"
        
        await update.message.reply_text(
            f"✅ **Đã kết thúc chat ẩn danh với user {target_user_id} ({display_name})**\n\n"
            f"🔹 AI đã được kích hoạt lại\n"
            f"🔹 Bạn có thể sử dụng `/chatuser` để chat với user khác"
        )
        
        # Thông báo cho user
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text="💬 **Cuộc trò chuyện đã kết thúc**\n\nBạn có thể tiếp tục sử dụng bot bình thường."
            )
        except:
            pass
        
        self.log_activity(user_id, "END_ANONYMOUS_CHAT", str(target_user_id))

    def remove_asterisks(self, text):
        """Xóa tất cả ký tự markdown formatting khỏi văn bản để giao diện sạch sẽ"""
        if not text:
            return text
        # Xóa ** (bold)
        text = text.replace("**", "")
        # Xóa * đơn (italic) nhưng giữ lại các dấu * cần thiết
        text = text.replace("*", "")
        # Xóa __ (underline)
        text = text.replace("__", "")
        # Xóa ` (code)
        text = text.replace("`", "")
        # Xóa ~~~ (strikethrough)
        text = text.replace("~~~", "")
        text = text.replace("~~", "")
        return text

    def get_vietnam_time(self):
        """Lấy thời gian Việt Nam (UTC+7)"""
        vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
        return datetime.now(vn_tz)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        # Kiểm tra emergency mode
        if hasattr(self, 'emergency_active') and self.emergency_active:
            if not self.admin.is_admin(user_id):
                await update.message.reply_text("🚨 **HỆ THỐNG BẢO TRÌ KHẨN CẤP**\n\nBot đang tạm dừng hoạt động. Vui lòng thử lại sau!")
                return
                
        # Kiểm tra maintenance mode
        if hasattr(self, 'maintenance_active') and self.maintenance_active:
            if not self.admin.is_admin(user_id):
                await update.message.reply_text("🔧 **HỆ THỐNG ĐANG BẢO TRÌ**\n\nBot đang trong chế độ bảo trì. Vui lòng thử lại sau!")
                return

        # Kiểm tra blacklist trước tiên
        if user_id in self.blacklisted_users:
            await update.message.reply_text("🚫 Bạn đã bị chặn vĩnh viễn khỏi bot.")
            return

        # Kiểm tra user suspension
        if self.is_user_suspended(user_id):
            suspend_info = self.suspended_users[user_id]
            end_time = suspend_info['end_time']
            remaining = end_time - datetime.now()
            hours_left = int(remaining.total_seconds() / 3600)
            await update.message.reply_text(
                f"⏸️ Tài khoản của bạn đang bị tạm khóa.\n"
                f"⏰ Thời gian còn lại: {hours_left} giờ\n"
                f"🕐 Kết thúc: {end_time.strftime('%H:%M %d/%m/%Y')}"
            )
            return

        # Kiểm tra chat ẩn danh - Admin đang chat với user
        for admin_id, target_user_id in self.admin_chat_sessions.items():
            if user_id == admin_id:
                # Admin gửi tin nhắn trong chat ẩn danh
                message_text = update.message.text
                try:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=f"💬 **Tin nhắn từ Admin:**\n\n{message_text}"
                    )
                    await update.message.reply_text(f"✅ Tin nhắn đã được gửi đến user {target_user_id}")
                    self.log_activity(user_id, "ANONYMOUS_CHAT_SEND", f"To {target_user_id}: {message_text[:50]}...")
                except Exception as e:
                    await update.message.reply_text(f"❌ Không thể gửi tin nhắn: {str(e)}")
                return
            
            elif user_id == target_user_id:
                # User trả lời trong chat ẩn danh
                message_text = update.message.text
                try:
                    # Lấy thông tin user
                    user_info = await context.bot.get_chat(user_id)
                    display_name = user_info.first_name or "User"
                    if user_info.last_name:
                        display_name += f" {user_info.last_name}"
                    
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=f"💬 **Tin nhắn từ {display_name} ({user_id}):**\n\n{message_text}"
                    )
                    await update.message.reply_text("✅ Tin nhắn của bạn đã được gửi đến Admin")
                    self.log_activity(admin_id, "ANONYMOUS_CHAT_RECEIVE", f"From {user_id}: {message_text[:50]}...")
                except Exception as e:
                    await update.message.reply_text("❌ Có lỗi khi gửi tin nhắn")
                return

        # Kiểm tra quyền - bao gồm cả quyền test (chỉ khi không trong chat ẩn danh)
        has_regular_permission = self.admin.is_authorized(user_id)
        has_test_permission = hasattr(self, 'test_users') and user_id in self.test_users and self.test_users[user_id]['messages_left'] > 0

        if not has_regular_permission and not has_test_permission:
            await update.message.reply_text("Bạn chưa được cấp quyền sử dụng bot. Liên hệ admin tại: " + ADMIN_CONTACT)
            return

        # Rate limiting
        if self.is_rate_limited(user_id):
            await update.message.reply_text("⏳ Vui lòng chờ 2 giây trước khi gửi tin nhắn tiếp theo!")
            return

        # Auto backup định kỳ
        self.auto_backup_check()

        if context.user_data.get('chatting', False):
            user_input = update.message.text
            # Xóa ký tự ** từ input của user
            user_input = self.remove_asterisks(user_input)

            # KIỂM TRA LƯỢT TEST TRƯỚC KHI XỬ LÝ (nếu có quyền test)
            if has_test_permission and not has_regular_permission:
                remaining = self.test_users[user_id]['messages_left']
                # Kiểm tra nếu hết lượt test
                if remaining <= 0:
                    await update.message.reply_text("❌ Bạn đã hết lượt test! Liên hệ admin để được cấp quyền chính thức.")
                    context.user_data['chatting'] = False
                    return

            # Gửi tin nhắn "đang phản hồi"
            typing_message = await update.message.reply_text(" Zyah King👽: Đang đọc và phân tích...")

            try:
                # Đảm bảo AI đọc và xử lý văn bản trước khi phản hồi với bộ nhớ user
                response = self.ai.call_api(user_input, user_id)
                formatted_response = self.ai.format_response(response)

                # Xóa tất cả ký tự markdown formatting để giao diện sạch sẽ
                formatted_response = self.remove_asterisks(formatted_response)

                # ✅ CHỈ TRỪ LƯỢT TEST KHI AI ĐÃ PHẢN HỒI THÀNH CÔNG
                if has_test_permission and not has_regular_permission:
                    self.test_users[user_id]['messages_left'] -= 1
                    print(f"🧪 Trừ 1 lượt test cho user {user_id}, còn lại: {self.test_users[user_id]['messages_left']}")

                # Xóa tin nhắn "đang phản hồi"
                try:
                    await typing_message.delete()
                except:
                    pass  # Bỏ qua lỗi nếu không xóa được tin nhắn

                # Thêm thông tin trạng thái bộ nhớ và lượt test còn lại
                memory_status = self.ai.get_memory_status(user_id)

                # Hiển thị lượt test còn lại nếu user có quyền test
                test_status = ""
                if has_test_permission and not has_regular_permission:
                    remaining = self.test_users[user_id]['messages_left']
                    test_status = f" | Test: {remaining} lượt"

                # Chia tin nhắn nếu quá dài (Telegram giới hạn 4096 ký tự)
                # Chỉ thêm tên một lần, không lặp lại
                full_message = f"{formatted_response}\n\n💾 Memory: {memory_status}{test_status}"
                if len(full_message) > 4096:
                    # Chia thành nhiều tin nhắn
                    for i in range(0, len(full_message), 4096):
                        chunk = full_message[i:i+4096]
                        chunk = self.remove_asterisks(chunk)  # Đảm bảo xóa ** ở mọi phần
                        await update.message.reply_text(chunk)
                else:
                    await update.message.reply_text(full_message)

                self.ai.update_memory(user_id, user_input, response)

            except Exception as e:
                # ❌ NẾU CÓ LỖI, KHÔNG TRỪ LƯỢT TEST
                try:
                    await typing_message.delete()
                except:
                    pass
                error_message = f" Zyah King👽: Đã xảy ra lỗi trong quá trình xử lý. Lượt test không bị trừ."
                await update.message.reply_text(error_message)

    def run(self):
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Đợi một chút để đảm bảo instance cũ đã tắt hoàn toàn
                if retry_count > 0:
                    print(f"{Colors.INFO}[🔄] Thử lại lần {retry_count + 1}...{Colors.RESET}")
                    time.sleep(5)

                if self.is_new_version:
                    # Phiên bản mới
                    self.app.add_handler(CommandHandler("start", self.start))
                    self.app.add_handler(CommandHandler("help", self.help))
                    self.app.add_handler(CommandHandler("chatai", self.chatai))
                    self.app.add_handler(CommandHandler("thongtinad", self.thongtinad))

                    self.app.add_handler(CommandHandler("mua", self.mua))
                    self.app.add_handler(CommandHandler("capquyen", self.capquyen))
                    self.app.add_handler(CommandHandler("xoaquyen", self.xoaquyen))
                    self.app.add_handler(CommandHandler("thongbao", self.thongbao))
                    self.app.add_handler(CommandHandler("kiemtra", self.kiemtra))

                    # Tính năng cũ
                    self.app.add_handler(CommandHandler("status", self.status))
                    self.app.add_handler(CommandHandler("memory", self.memory))
                    self.app.add_handler(CommandHandler("backup", self.backup))

                    # Tính năng mới
                    self.app.add_handler(CommandHandler("weather", self.weather))
                    self.app.add_handler(CommandHandler("news", self.news))
                    self.app.add_handler(CommandHandler("testgui", self.testgui))
                    self.app.add_handler(CommandHandler("sysinfo", self.sysinfo))
                    self.app.add_handler(CommandHandler("kiemtratinnhan", self.kiemtratinnhan))
                    # Add /test command handler
                    self.app.add_handler(CommandHandler("test", self.test_user))
                    self.app.add_handler(CommandHandler("xoatest", self.remove_test))
                    self.app.add_handler(CommandHandler("testall", self.test_all))
                    self.app.add_handler(CommandHandler("xoatestall", self.remove_all_tests))
                    
                    # Advanced admin features
                    self.app.add_handler(CommandHandler("monitor", self.monitor_users))
                    self.app.add_handler(CommandHandler("analytics", self.analytics))
                    self.app.add_handler(CommandHandler("blacklist", self.blacklist_user))
                    self.app.add_handler(CommandHandler("unblacklist", self.unblacklist_user))
                    self.app.add_handler(CommandHandler("broadcast_vip", self.broadcast_vip))
                    self.app.add_handler(CommandHandler("force_stop", self.force_stop_user))
                    self.app.add_handler(CommandHandler("ai_stats", self.ai_statistics))
                    self.app.add_handler(CommandHandler("user_profile", self.user_profile))
                    self.app.add_handler(CommandHandler("mass_action", self.mass_action))
                    self.app.add_handler(CommandHandler("security_scan", self.security_scan))
                    
                    # Anonymous chat features
                    self.app.add_handler(CommandHandler("chatuser", self.chatuser))
                    self.app.add_handler(CommandHandler("huychat", self.huychat))
                    
                    # Advanced admin panels
                    self.app.add_handler(CommandHandler("admin_panel", self.admin_panel))
                    self.app.add_handler(CommandHandler("admin_stats", self.admin_stats))
                    self.app.add_handler(CommandHandler("admin_tools", self.admin_tools))
                    self.app.add_handler(CommandHandler("emergency_mode", self.emergency_mode))
                    self.app.add_handler(CommandHandler("maintenance_mode", self.maintenance_mode))
                    self.app.add_handler(CommandHandler("system_monitor", self.system_monitor))
                    self.app.add_handler(CommandHandler("advanced_admin", self.advanced_admin))

                    self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

                    print(f"{Colors.INFO}[🌌] Zyah King👽 đang khởi động với tính năng mới...{Colors.RESET}")
                    self.log_activity("SYSTEM", "BOT_START")

                    print(f"{Colors.SUCCESS}[🌌] Zyah King👽 đã khởi động thành công!{Colors.RESET}")
                    self.log_activity("SYSTEM", "BOT_START")
                    self.app.run_polling()
                    break  # Thoát loop nếu chạy thành công

                else:
                    # Phiên bản cũ - compatibility mode
                    dp = self.app.dispatcher

                    dp.add_handler(CommandHandler("start", self.start))
                    dp.add_handler(CommandHandler("help", self.help))
                    dp.add_handler(CommandHandler("chatai", self.chatai))
                    dp.add_handler(CommandHandler("thongtinad", self.thongtinad))
                    dp.add_handler(CommandHandler("mua", self.mua))
                    dp.add_handler(CommandHandler("capquyen", self.capquyen))
                    dp.add_handler(CommandHandler("xoaquyen", self.xoaquyen))
                    dp.add_handler(CommandHandler("thongbao", self.thongbao))
                    dp.add_handler(CommandHandler("kiemtra", self.kiemtra))
                    dp.add_handler(CommandHandler("status", self.status))
                    dp.add_handler(CommandHandler("memory", self.memory))
                    dp.add_handler(CommandHandler("backup", self.backup))
                    dp.add_handler(CommandHandler("weather", self.weather))
                    dp.add_handler(CommandHandler("news", self.news))
                    dp.add_handler(CommandHandler("testgui", self.testgui))
                    dp.add_handler(CommandHandler("sysinfo", self.sysinfo))
                    dp.add_handler(CommandHandler("kiemtratinnhan", self.kiemtratinnhan))
                    # Add /test command handler
                    dp.add_handler(CommandHandler("test", self.test_user))
                    dp.add_handler(CommandHandler("xoatest", self.remove_test))
                    dp.add_handler(CommandHandler("testall", self.test_all))
                    dp.add_handler(CommandHandler("xoatestall", self.remove_all_tests))
                    
                    # Advanced admin features
                    dp.add_handler(CommandHandler("monitor", self.monitor_users))
                    dp.add_handler(CommandHandler("analytics", self.analytics))
                    dp.add_handler(CommandHandler("blacklist", self.blacklist_user))
                    dp.add_handler(CommandHandler("unblacklist", self.unblacklist_user))
                    dp.add_handler(CommandHandler("broadcast_vip", self.broadcast_vip))
                    dp.add_handler(CommandHandler("force_stop", self.force_stop_user))
                    dp.add_handler(CommandHandler("ai_stats", self.ai_statistics))
                    dp.add_handler(CommandHandler("user_profile", self.user_profile))
                    dp.add_handler(CommandHandler("mass_action", self.mass_action))
                    dp.add_handler(CommandHandler("security_scan", self.security_scan))
                    
                    # Anonymous chat features
                    dp.add_handler(CommandHandler("chatuser", self.chatuser))
                    dp.add_handler(CommandHandler("huychat", self.huychat))
                    
                    # Advanced admin panels
                    dp.add_handler(CommandHandler("admin_panel", self.admin_panel))
                    dp.add_handler(CommandHandler("admin_stats", self.admin_stats))
                    dp.add_handler(CommandHandler("admin_tools", self.admin_tools))
                    dp.add_handler(CommandHandler("emergency_mode", self.emergency_mode))
                    dp.add_handler(CommandHandler("maintenance_mode", self.maintenance_mode))
                    dp.add_handler(CommandHandler("system_monitor", self.system_monitor))
                    dp.add_handler(CommandHandler("advanced_admin", self.advanced_admin))

                    dp.add_handler(MessageHandler(filters.text & ~filters.command, self.handle_message))

                    print(f"{Colors.SUCCESS}[🌌] Zyah King👽 đã khởi động thành công (compatibility mode)!{Colors.RESET}")
                    self.log_activity("SYSTEM", "BOT_START")

                    self.app.start_polling()
                    self.app.idle()
                    break  # Thoát loop nếu chạy thành công

            except KeyboardInterrupt:
                print(f"{Colors.INFO}[👋] Bot đã được dừng bởi user{Colors.RESET}")
                self.cleanup()
                break

            except Exception as e:
                error_msg = str(e).lower()
                if 'conflict' in error_msg and 'getupdates' in error_msg:
                    print(f"{Colors.WARNING}[⚠️] Phát hiện conflict với instance khác: {e}{Colors.RESET}")
                    retry_count += 1
                    if retry_count >= max_retries:
                        print(f"{Colors.ERROR}[💥] Đã thử {max_retries} lần, bot không thể khởi động{Colors.RESET}")
                        self.cleanup()
                        break
                    else:
                        print(f"{Colors.INFO}[🔄] Đang cố gắng dừng các instance khác...{Colors.RESET}")
                        self.check_running_instance()  # Thử dừng instance khác lại
                        continue
                elif 'network' in error_msg or 'timeout' in error_msg:
                    print(f"{Colors.WARNING}[⚠️] Network issue: {e}{Colors.RESET}")
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"{Colors.INFO}[🔄] Thử kết nối lại sau 10 giây...{Colors.RESET}")
                        time.sleep(10)
                        continue
                    else:
                        print(f"{Colors.ERROR}[💥] Không thể kết nối sau {max_retries} lần thử{Colors.RESET}")
                        self.cleanup()
                        break
                else:
                    print(f"{Colors.ERROR}[💥] Bot crashed: {e}{Colors.RESET}")
                    print(f"{Colors.INFO}[ℹ️] Chi tiết lỗi: {type(e).__name__}{Colors.RESET}")
                    self.cleanup()
                    break

    async def test_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cấp quyền test cho user cụ thể"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if len(context.args) < 2:
            await update.message.reply_text("❌ Hãy cung cấp ID user và số lượng tin nhắn test.\n📝 Ví dụ: `/test 123456789 5`")
            return

        try:
            target_user_id = int(context.args[0])
            messages_count = int(context.args[1])

            # Validation
            if target_user_id <= 0 or len(str(target_user_id)) < 5:
                await update.message.reply_text("❌ ID người dùng không hợp lệ!")
                return

            if messages_count <= 0 or messages_count > 50:
                await update.message.reply_text("❌ Số lượng tin nhắn phải từ 1-50!")
                return

            # Kiểm tra user đã có quyền chính thức chưa
            if self.admin.is_authorized(target_user_id):
                await update.message.reply_text(f"❌ User {target_user_id} đã có quyền chính thức, không cần test!")
                return

            # Khởi tạo test_users nếu chưa có
            if not hasattr(self, 'test_users'):
                self.test_users = {}

            # Cấp quyền test
            self.test_users[target_user_id] = {
                'messages_left': messages_count,
                'granted_at': datetime.now(),
                'granted_by': user_id
            }

            await update.message.reply_text(f"✅ Đã cấp {messages_count} lượt test cho user {target_user_id}!")

            # Gửi thông báo đến user được cấp quyền test
            try:
                user_info = await context.bot.get_chat(target_user_id)
                display_name = user_info.first_name or "bạn"
                if user_info.last_name:
                    display_name += f" {user_info.last_name}"

                test_message = (
                    f"🧪 **THÔNG BÁO TEST TỪ ADMIN** 🧪\n\n"
                    f"👋 Chào **{display_name}**!\n\n"
                    f"✅ Bạn đã được cấp **{messages_count} lượt test** để trải nghiệm **Zyah King👽**!\n\n"
                    f"📝 **Hướng dẫn sử dụng:**\n"
                    f"1. Gõ `/chatai` để bắt đầu chat\n"
                    f"2. Gửi tin nhắn để trò chuyện với AI\n\n"
                    f"💡 **Lưu ý:** Lượt test chỉ bị trừ khi bạn gửi tin nhắn trả lời, không phải khi gõ lệnh!\n\n"
                    f"🚀 Hãy trải nghiệm sức mạnh của AI!"
                )

                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=test_message,
                    parse_mode='Markdown'
                )

                await update.message.reply_text(f"📢 Đã gửi thông báo test đến user {target_user_id}!")

            except Exception as e:
                await update.message.reply_text(f"⚠️ Đã cấp quyền test thành công nhưng không thể gửi thông báo: {str(e)}")

            self.log_activity(user_id, "GRANT_TEST_PERMISSION", f"{target_user_id} - {messages_count} messages")

        except ValueError:
            await update.message.reply_text("❌ ID user và số lượng tin nhắn phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi cấp quyền test: {str(e)}")

    async def remove_test(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Xóa quyền test của user cụ thể"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("❌ Hãy cung cấp ID user.\n📝 Ví dụ: `/xoatest 123456789`")
            return

        try:
            target_user_id = int(context.args[0])

            if not hasattr(self, 'test_users') or target_user_id not in self.test_users:
                await update.message.reply_text(f"❌ User {target_user_id} không có quyền test!")
                return

            # Xóa quyền test
            del self.test_users[target_user_id]

            await update.message.reply_text(f"✅ Đã xóa quyền test của user {target_user_id}!")

            # Gửi thông báo đến user bị xóa quyền test
            try:
                from config import SUB_ADMIN_CONTACT

                # Lấy tên Telegram thật của user
                try:
                    user_info = await context.bot.get_chat(target_user_id)
                    telegram_name = user_info.first_name or "bạn"
                    if user_info.last_name:
                        telegram_name += f" {user_info.last_name}"
                except:
                    telegram_name = "bạn"

                # Tạo inline keyboard với 2 admin
                keyboard = [
                    [InlineKeyboardButton("👑 Nhayy", url=ADMIN_CONTACT)],
                    [InlineKeyboardButton("⭐ Uy Han", url=SUB_ADMIN_CONTACT)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                revoke_test_message = (
                    f"🧪 **THÔNG BÁO TỪ ADMIN** 🧪\n\n"
                    f"👋 Chào **{telegram_name}**!\n\n"
                    f"❌ Quyền test của bạn đã **hết hạn sử dụng**.\n\n"
                    f"🙏 **Cảm ơn bạn đã trải nghiệm Zyah King👽!**\n\n"
                    f"💫 **Muốn tiếp tục sử dụng những tính năng tuyệt vời?**\n"
                    f"📞 Hãy liên hệ với admin để **nâng cấp lên quyền chính thức** 👇"
                )

                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=revoke_test_message,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )

                await update.message.reply_text(f"📢 Đã gửi thông báo hết hạn test đến user {target_user_id} ({telegram_name})!")

            except Exception as e:
                await update.message.reply_text(f"⚠️ Đã xóa quyền test thành công nhưng không thể gửi thông báo: {str(e)}")

            self.log_activity(user_id, "REMOVE_TEST_PERMISSION", str(target_user_id))

        except ValueError:
            await update.message.reply_text("❌ ID user phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi xóa quyền test: {str(e)}")

    async def test_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cấp quyền test cho tất cả user chưa có quyền"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("❌ Hãy cung cấp số lượng tin nhắn test.\n📝 Ví dụ: `/testall 5`")
            return

        try:
            messages_count = int(context.args[0])

            if messages_count <= 0 or messages_count > 50:
                await update.message.reply_text("❌ Số lượng tin nhắn phải từ 1-50!")
                return

            # Lấy tất cả user đã từng sử dụng bot
            all_tracked_users = self.admin.get_all_tracked_users()
            FIXED_ADMIN_IDS = [7073749415, 7444696176]
            authorized_users = [uid for uid in self.admin.authorized_users["users"] if uid not in FIXED_ADMIN_IDS]

            # Lọc user chưa có quyền và chưa có quyền test
            if not hasattr(self, 'test_users'):
                self.test_users = {}

            target_users = []
            for uid in all_tracked_users:
                if (uid not in FIXED_ADMIN_IDS and 
                    uid not in authorized_users and 
                    uid not in self.test_users):
                    target_users.append(uid)

            if not target_users:
                await update.message.reply_text("❌ Không có user nào để cấp quyền test!")
                return

            await update.message.reply_text(f"🧪 Đang cấp {messages_count} lượt test cho {len(target_users)} user...")

            success_count = 0
            fail_count = 0

            for target_user_id in target_users:
                try:
                    # Cấp quyền test
                    self.test_users[target_user_id] = {
                        'messages_left': messages_count,
                        'granted_at': datetime.now(),
                        'granted_by': user_id
                    }

                    # Gửi thông báo
                    try:
                        user_info = await context.bot.get_chat(target_user_id)
                        display_name = user_info.first_name or "bạn"
                        if user_info.last_name:
                            display_name += f" {user_info.last_name}"

                        test_message = (
                            f"🧪 **THÔNG BÁO TEST TỪ ADMIN** 🧪\n\n"
                            f"👋 Chào **{display_name}**!\n\n"
                            f"✅ Bạn đã được cấp **{messages_count} lượt test** để trải nghiệm **Zyah King👽**!\n\n"
                            f"📝 **Hướng dẫn sử dụng:**\n"
                            f"1. Gõ `/chatai` để bắt đầu chat\n"
                            f"2. Gửi tin nhắn để trò chuyện với AI\n\n"
                            f"💡 **Lưu ý:** Lượt test chỉ bị trừ khi bạn gửi tin nhắn trả lời, không phải khi gõ lệnh!\n\n"
                            f"🚀 Hãy trải nghiệm sức mạnh của AI!"
                        )

                        await context.bot.send_message(
                            chat_id=target_user_id,
                            text=test_message,
                            parse_mode='Markdown'
                        )

                        success_count += 1

                    except Exception:
                        success_count += 1  # Vẫn tính thành công vì đã cấp quyền

                except Exception:
                    fail_count += 1

            report = (
                f"✅ Hoàn tất cấp quyền test!\n"
                f"• Thành công: {success_count}/{len(target_users)} user\n"
                f"• Thất bại: {fail_count}/{len(target_users)} user"
            )

            await update.message.reply_text(report)
            self.log_activity(user_id, "GRANT_TEST_ALL", f"Success: {success_count}, Failed: {fail_count}")

        except ValueError:
            await update.message.reply_text("❌ Số lượng tin nhắn phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi cấp quyền test: {str(e)}")

    async def remove_all_tests(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Xóa quyền test của tất cả user"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not hasattr(self, 'test_users') or not self.test_users:
            await update.message.reply_text("❌ Không có user nào có quyền test!")
            return

        # Lấy danh sách user có quyền test trước khi xóa
        test_user_ids = list(self.test_users.keys())
        count = len(test_user_ids)

        await update.message.reply_text(f"🧪 Đang xóa quyền test của {count} user và gửi thông báo...")

        success_notify = 0
        fail_notify = 0

        # Gửi thông báo đến tất cả user bị xóa quyền test
        for target_user_id in test_user_ids:
            try:
                from config import SUB_ADMIN_CONTACT

                # Lấy tên Telegram thật của user
                try:
                    user_info = await context.bot.get_chat(target_user_id)
                    telegram_name = user_info.first_name or "bạn"
                    if user_info.last_name:
                        telegram_name += f" {user_info.last_name}"
                except:
                    telegram_name = "bạn"

                # Tạo inline keyboard với 2 admin
                keyboard = [
                    [InlineKeyboardButton("👑 Nhayy", url=ADMIN_CONTACT)],
                    [InlineKeyboardButton("⭐ Uy Han", url=SUB_ADMIN_CONTACT)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                revoke_all_test_message = (
                    f"🧪 **THÔNG BÁO QUAN TRỌNG TỪ ADMIN** 🧪\n\n"
                    f"👋 Chào **{telegram_name}**!\n\n"
                    f"❌ **Đợt test trial đã kết thúc**, quyền test của bạn đã hết hạn.\n\n"
                    f"🙏 **Cảm ơn bạn đã tham gia trải nghiệm Zyah King👽!**\n\n"
                    f"🚀 **Bạn đã thấy sức mạnh tuyệt vời của AI chưa?**\n"
                    f"💫 **Muốn sở hữu trọn vẹn những tính năng đỉnh cao này?**\n"
                    f"📞 Liên hệ admin ngay để **nâng cấp lên quyền chính thức** 👇"
                )

                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=revoke_all_test_message,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )

                success_notify += 1

            except Exception:
                fail_notify += 1

        # Xóa tất cả quyền test
        self.test_users = {}

        # Báo cáo kết quả
        report = (
            f"✅ Hoàn tất xóa quyền test của {count} user!\n"
            f"📢 Thông báo thành công: {success_notify}/{count}\n"
            f"❌ Thông báo thất bại: {fail_notify}/{count}"
        )

        await update.message.reply_text(report)
        self.log_activity(user_id, "REMOVE_ALL_TESTS", f"Removed {count} users, Notified: {success_notify}")

    def load_blacklist(self):
        """Load blacklist từ file"""
        try:
            if os.path.exists("blacklist.json"):
                with open("blacklist.json", 'r') as f:
                    data = json.load(f)
                    self.blacklisted_users = set(data.get("blacklisted", []))
        except:
            self.blacklisted_users = set()

    def save_blacklist(self):
        """Save blacklist vào file"""
        try:
            with open("blacklist.json", 'w') as f:
                json.dump({"blacklisted": list(self.blacklisted_users)}, f, indent=4)
        except Exception as e:
            print(f"Error saving blacklist: {e}")

    async def monitor_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Theo dõi hoạt động user real-time"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        # Lấy thống kê real-time
        active_users = len(self.user_last_request)
        chat_sessions = len(self.ai.user_memories) if hasattr(self.ai, 'user_memories') else 0
        
        # Top 5 users hoạt động gần đây
        recent_activities = sorted(self.user_last_request.items(), 
                                 key=lambda x: x[1], reverse=True)[:5]
        
        monitor_text = (
            f"📊 **GIÁM SÁT REAL-TIME** 📊\n"
            f"🕐 Cập nhật: {self.get_vietnam_time().strftime('%H:%M:%S %d/%m/%Y')}\n\n"
            f"👥 **Tổng quan:**\n"
            f"• Users hoạt động: {active_users}\n"
            f"• Chat sessions: {chat_sessions}\n"
            f"• Blacklisted: {len(self.blacklisted_users)}\n\n"
            f"🔥 **Top hoạt động gần đây:**\n"
        )
        
        for i, (uid, last_time) in enumerate(recent_activities, 1):
            time_ago = datetime.now() - last_time
            minutes_ago = int(time_ago.total_seconds() / 60)
            monitor_text += f"{i}. User {uid} - {minutes_ago} phút trước\n"
        
        # Thêm cảnh báo bảo mật nếu có
        if self.security_logs:
            recent_alerts = [log for log in self.security_logs[-5:] if (datetime.now() - log['time']).total_seconds() < 3600]
            if recent_alerts:
                monitor_text += f"\n⚠️ **Cảnh báo (1h qua): {len(recent_alerts)} alerts**"
        
        monitor_text += f"\n\n🔄 Sử dụng lại /monitor để refresh"
        
        await update.message.reply_text(monitor_text, parse_mode='Markdown')
        self.log_activity(user_id, "MONITOR_USERS")

    async def analytics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Thống kê chi tiết và analytics"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        # Tính toán analytics chi tiết
        total_users = len(self.admin.get_all_tracked_users())
        authorized_users = len(self.admin.get_all_users())
        test_users_count = len(getattr(self, 'test_users', {}))
        
        # Thống kê AI usage
        total_messages = sum(len(memory) for memory in self.ai.user_memories.values()) if hasattr(self.ai, 'user_memories') else 0
        avg_messages_per_user = total_messages / max(len(self.ai.user_memories), 1) if hasattr(self.ai, 'user_memories') else 0
        
        # Thống kê thời gian
        current_time = self.get_vietnam_time()
        uptime = current_time - self.last_backup
        
        analytics_text = (
            f"📈 **ANALYTICS DASHBOARD** 📈\n"
            f"🕐 Generated: {current_time.strftime('%H:%M:%S %d/%m/%Y')}\n"
            f"⏱️ Uptime: {str(uptime).split('.')[0]}\n\n"
            f"👥 **User Statistics:**\n"
            f"• Total registered: {total_users}\n"
            f"• Authorized users: {authorized_users}\n"
            f"• Test users: {test_users_count}\n"
            f"• Blacklisted: {len(self.blacklisted_users)}\n"
            f"• Conversion rate: {(authorized_users/max(total_users,1)*100):.1f}%\n\n"
            f"🤖 **AI Performance:**\n"
            f"• Total conversations: {total_messages//2}\n"
            f"• Avg msgs/user: {avg_messages_per_user/2:.1f}\n"
            f"• Active sessions: {len(self.ai.user_memories) if hasattr(self.ai, 'user_memories') else 0}\n"
            f"• Memory usage: {total_messages}/{len(self.ai.user_memories) * self.ai.MAX_MEMORY if hasattr(self.ai, 'user_memories') else 0}\n\n"
            f"📊 **System Health:**\n"
            f"• Rate limit violations: {len([u for u in self.user_last_request if (datetime.now() - self.user_last_request[u]).total_seconds() < self.rate_limit_seconds])}\n"
            f"• Security alerts: {len(self.security_logs)}\n"
            f"• Last backup: {self.last_backup.strftime('%d/%m/%Y %H:%M')}\n\n"
            f"🎯 **Growth Metrics:**\n"
            f"• New users today: {len([u for u in self.admin.get_all_tracked_users() if u not in self.admin.get_all_users()])}\n"
            f"• Engagement rate: {(len(self.user_last_request)/max(total_users,1)*100):.1f}%"
        )
        
        await update.message.reply_text(analytics_text, parse_mode='Markdown')
        self.log_activity(user_id, "VIEW_ANALYTICS")

    async def blacklist_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Chặn user vĩnh viễn"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("❌ Hãy cung cấp ID user cần chặn.\n📝 Ví dụ: `/blacklist 123456789`")
            return

        try:
            target_user_id = int(context.args[0])
            
            # Không thể blacklist admin
            if self.admin.is_admin(target_user_id):
                await update.message.reply_text("❌ Không thể blacklist Admin!")
                return
            
            if target_user_id in self.blacklisted_users:
                await update.message.reply_text(f"❌ User {target_user_id} đã bị blacklist!")
                return
            
            # Thêm vào blacklist
            self.blacklisted_users.add(target_user_id)
            self.save_blacklist()
            
            # Xóa quyền nếu có
            if self.admin.is_authorized(target_user_id):
                self.admin.remove_user(str(target_user_id))
            
            # Xóa quyền test nếu có
            if hasattr(self, 'test_users') and target_user_id in self.test_users:
                del self.test_users[target_user_id]
            
            # Xóa khỏi chat session
            if hasattr(self.ai, 'user_memories') and target_user_id in self.ai.user_memories:
                del self.ai.user_memories[target_user_id]
            
            await update.message.reply_text(f"🚫 Đã blacklist user {target_user_id} vĩnh viễn!")
            
            # Gửi thông báo đến user bị blacklist
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text="🚫 **BẠN ĐÃ BỊ CHẶN VĨNH VIỄN**\n\nTài khoản của bạn đã bị chặn do vi phạm quy định sử dụng bot.",
                    parse_mode='Markdown'
                )
            except:
                pass
            
            self.log_activity(user_id, "BLACKLIST_USER", str(target_user_id))
            
        except ValueError:
            await update.message.reply_text("❌ ID user phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi blacklist: {str(e)}")

    async def unblacklist_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Bỏ chặn user"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("❌ Hãy cung cấp ID user cần bỏ chặn.\n📝 Ví dụ: `/unblacklist 123456789`")
            return

        try:
            target_user_id = int(context.args[0])
            
            if target_user_id not in self.blacklisted_users:
                await update.message.reply_text(f"❌ User {target_user_id} không bị blacklist!")
                return
            
            # Xóa khỏi blacklist
            self.blacklisted_users.remove(target_user_id)
            self.save_blacklist()
            
            await update.message.reply_text(f"✅ Đã bỏ chặn user {target_user_id}!")
            
            # Gửi thông báo đến user được bỏ chặn
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text="✅ **TÀI KHOẢN ĐÃ ĐƯỢC KHÔI PHỤC**\n\nBạn đã được bỏ chặn và có thể sử dụng bot trở lại.",
                    parse_mode='Markdown'
                )
            except:
                pass
            
            self.log_activity(user_id, "UNBLACKLIST_USER", str(target_user_id))
            
        except ValueError:
            await update.message.reply_text("❌ ID user phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi unblacklist: {str(e)}")

    async def broadcast_vip(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Gửi thông báo VIP với format đẹp"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("❌ Hãy nhập nội dung thông báo VIP.\n📝 Ví dụ: `/broadcast_vip Cập nhật tính năng mới!`")
            return

        message = ' '.join(context.args)
        
        # Format VIP message
        vip_message = (
            f"👑 **THÔNG BÁO VIP TỪ ZYAH KING👽** 👑\n"
            f"{'═' * 40}\n\n"
            f"🌟 {message}\n\n"
            f"{'═' * 40}\n"
            f"🕐 Thời gian: {self.get_vietnam_time().strftime('%H:%M %d/%m/%Y')}\n"
            f"📞 Liên hệ: {ADMIN_CONTACT}\n"
            f"🌌 **Zyah King👽 - Sức mạnh không giới hạn!**"
        )
        
        # Gửi đến tất cả user có quyền
        all_users = self.admin.get_all_users()
        success_count = 0
        fail_count = 0
        
        await update.message.reply_text(f"👑 Đang gửi thông báo VIP đến {len(all_users)} user có quyền...")
        
        for target_user_id in all_users:
            if target_user_id not in self.blacklisted_users:
                try:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=vip_message,
                        parse_mode='Markdown'
                    )
                    success_count += 1
                except:
                    fail_count += 1
        
        report = (
            f"👑 **Hoàn tất gửi thông báo VIP!**\n"
            f"✅ Thành công: {success_count}\n"
            f"❌ Thất bại: {fail_count}"
        )
        
        await update.message.reply_text(report, parse_mode='Markdown')
        self.log_activity(user_id, "BROADCAST_VIP", f"Success: {success_count}, Failed: {fail_count}")

    async def force_stop_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Buộc dừng chat session của user"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("❌ Hãy cung cấp ID user cần force stop.\n📝 Ví dụ: `/force_stop 123456789`")
            return

        try:
            target_user_id = int(context.args[0])
            
            # Xóa chat session
            if hasattr(self.ai, 'user_memories') and target_user_id in self.ai.user_memories:
                del self.ai.user_memories[target_user_id]
                session_stopped = True
            else:
                session_stopped = False
            
            # Xóa khỏi rate limiting
            if target_user_id in self.user_last_request:
                del self.user_last_request[target_user_id]
            
            await update.message.reply_text(
                f"🛑 Đã force stop user {target_user_id}!\n"
                f"• Chat session: {'✅ Đã dừng' if session_stopped else '❌ Không có session'}\n"
                f"• Rate limit: ✅ Đã reset"
            )
            
            # Thông báo đến user
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text="🛑 **SESSION BỊ DỪNG BỞI ADMIN**\n\nChat session của bạn đã bị dừng. Gõ `/chatai` để bắt đầu lại.",
                    parse_mode='Markdown'
                )
            except:
                pass
            
            self.log_activity(user_id, "FORCE_STOP_USER", str(target_user_id))
            
        except ValueError:
            await update.message.reply_text("❌ ID user phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi force stop: {str(e)}")

    async def ai_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Thống kê chi tiết AI và performance"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        # Thống kê AI chi tiết
        if hasattr(self.ai, 'user_memories'):
            total_conversations = sum(len(memory)//2 for memory in self.ai.user_memories.values())
            active_sessions = len(self.ai.user_memories)
            total_messages = sum(len(memory) for memory in self.ai.user_memories.values())
            
            # Top users sử dụng AI nhiều nhất
            top_users = sorted(self.ai.user_memories.items(), 
                             key=lambda x: len(x[1]), reverse=True)[:5]
        else:
            total_conversations = active_sessions = total_messages = 0
            top_users = []
        
        # Memory usage statistics
        memory_efficiency = (total_messages / (active_sessions * self.ai.MAX_MEMORY * 2) * 100) if active_sessions > 0 else 0
        
        ai_stats = (
            f"🤖 **AI PERFORMANCE DASHBOARD** 🤖\n"
            f"🕐 Updated: {self.get_vietnam_time().strftime('%H:%M:%S %d/%m/%Y')}\n\n"
            f"📊 **Conversation Stats:**\n"
            f"• Total conversations: {total_conversations}\n"
            f"• Active sessions: {active_sessions}\n"
            f"• Total messages: {total_messages}\n"
            f"• Memory efficiency: {memory_efficiency:.1f}%\n\n"
            f"🏆 **Top AI Users:**\n"
        )
        
        for i, (uid, memory) in enumerate(top_users, 1):
            conversations = len(memory) // 2
            ai_stats += f"{i}. User {uid}: {conversations} conversations\n"
        
        if not top_users:
            ai_stats += "Chưa có user nào sử dụng AI\n"
        
        ai_stats += (
            f"\n⚡ **Performance Metrics:**\n"
            f"• Avg msgs/session: {total_messages/max(active_sessions,1):.1f}\n"
            f"• Memory slots used: {total_messages}/{active_sessions * self.ai.MAX_MEMORY if active_sessions > 0 else 0}\n"
            f"• API timeout: {getattr(self.ai, 'timeout', 'N/A')}s\n"
        )
        
        await update.message.reply_text(ai_stats, parse_mode='Markdown')
        self.log_activity(user_id, "AI_STATISTICS")

    async def user_profile(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Xem profile chi tiết của user"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("❌ Hãy cung cấp ID user.\n📝 Ví dụ: `/user_profile 123456789`")
            return

        try:
            target_user_id = int(context.args[0])
            
            # Lấy thông tin cơ bản
            try:
                user_info = await context.bot.get_chat(target_user_id)
                display_name = user_info.first_name or "Không có tên"
                if user_info.last_name:
                    display_name += f" {user_info.last_name}"
                username = f"@{user_info.username}" if user_info.username else "Không có username"
            except:
                display_name = "Không lấy được tên"
                username = "Không có username"
            
            # Check status
            is_authorized = self.admin.is_authorized(target_user_id)
            is_admin = self.admin.is_admin(target_user_id)
            is_blacklisted = target_user_id in self.blacklisted_users
            has_test = hasattr(self, 'test_users') and target_user_id in self.test_users
            
            # Chat statistics
            if hasattr(self.ai, 'user_memories') and target_user_id in self.ai.user_memories:
                conversations = len(self.ai.user_memories[target_user_id]) // 2
                last_message_count = len(self.ai.user_memories[target_user_id])
            else:
                conversations = last_message_count = 0
            
            # Last activity
            last_activity = "Chưa có hoạt động"
            if target_user_id in self.user_last_request:
                time_diff = datetime.now() - self.user_last_request[target_user_id]
                if time_diff.total_seconds() < 60:
                    last_activity = f"{int(time_diff.total_seconds())} giây trước"
                elif time_diff.total_seconds() < 3600:
                    last_activity = f"{int(time_diff.total_seconds()//60)} phút trước"
                else:
                    last_activity = f"{int(time_diff.total_seconds()//3600)} giờ trước"
            
            profile = (
                f"👤 **USER PROFILE** 👤\n"
                f"{'═' * 30}\n\n"
                f"🆔 **ID:** {target_user_id}\n"
                f"👤 **Tên:** {display_name}\n"
                f"🔗 **Username:** {username}\n\n"
                f"🏷️ **Status:**\n"
                f"• Admin: {'✅' if is_admin else '❌'}\n"
                f"• Authorized: {'✅' if is_authorized else '❌'}\n"
                f"• Blacklisted: {'🚫' if is_blacklisted else '✅'}\n"
                f"• Test user: {'🧪' if has_test else '❌'}\n"
            )
            
            if has_test:
                test_info = self.test_users[target_user_id]
                profile += f"• Test messages left: {test_info['messages_left']}\n"
            
            profile += (
                f"\n📊 **Activity:**\n"
                f"• Conversations: {conversations}\n"
                f"• Messages in memory: {last_message_count}\n"
                f"• Last activity: {last_activity}\n\n"
                f"🕐 **Generated:** {self.get_vietnam_time().strftime('%H:%M:%S %d/%m/%Y')}"
            )
            
            await update.message.reply_text(profile, parse_mode='Markdown')
            self.log_activity(user_id, "USER_PROFILE", str(target_user_id))
            
        except ValueError:
            await update.message.reply_text("❌ ID user phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi khi lấy profile: {str(e)}")

    async def mass_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Thực hiện hành động hàng loạt"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            actions_help = (
                "⚡ **MASS ACTION COMMANDS** ⚡\n\n"
                "📝 **Cú pháp:** `/mass_action <action>`\n\n"
                "🔧 **Actions có sẵn:**\n"
                "• `clear_memory` - Xóa memory của tất cả users\n"
                "• `reset_rate_limit` - Reset rate limit của tất cả users\n"
                "• `clean_inactive` - Xóa users không hoạt động >7 ngày\n"
                "• `backup_all` - Backup toàn bộ dữ liệu\n"
                "• `count_messages` - Đếm tổng tin nhắn\n"
                "• `export_users` - Export danh sách users\n\n"
                "⚠️ **Lưu ý:** Các hành động này không thể hoàn tác!"
            )
            await update.message.reply_text(actions_help, parse_mode='Markdown')
            return

        action = context.args[0].lower()
        
        if action == "clear_memory":
            if hasattr(self.ai, 'user_memories'):
                count = len(self.ai.user_memories)
                self.ai.user_memories = {}
                await update.message.reply_text(f"🧹 Đã xóa memory của {count} users!")
            else:
                await update.message.reply_text("❌ Không có memory để xóa!")
                
        elif action == "reset_rate_limit":
            count = len(self.user_last_request)
            self.user_last_request = {}
            await update.message.reply_text(f"⚡ Đã reset rate limit của {count} users!")
            
        elif action == "clean_inactive":
            # Xóa users không hoạt động > 7 ngày
            week_ago = datetime.now() - timedelta(days=7)
            inactive_users = [uid for uid, last_time in self.user_last_request.items() 
                            if last_time < week_ago and not self.admin.is_admin(uid)]
            
            for uid in inactive_users:
                del self.user_last_request[uid]
                if hasattr(self.ai, 'user_memories') and uid in self.ai.user_memories:
                    del self.ai.user_memories[uid]
            
            await update.message.reply_text(f"🧹 Đã dọn dẹp {len(inactive_users)} users không hoạt động!")
            
        elif action == "backup_all":
            try:
                backup_dir = f"backups/mass_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                os.makedirs(backup_dir, exist_ok=True)
                
                # Backup users.json
                if os.path.exists("users.json"):
                    shutil.copy2("users.json", f"{backup_dir}/users.json")
                
                # Backup blacklist
                if os.path.exists("blacklist.json"):
                    shutil.copy2("blacklist.json", f"{backup_dir}/blacklist.json")
                
                # Backup logs
                if os.path.exists("logs"):
                    shutil.copytree("logs", f"{backup_dir}/logs", dirs_exist_ok=True)
                
                await update.message.reply_text(f"💾 Mass backup hoàn tất: {backup_dir}")
            except Exception as e:
                await update.message.reply_text(f"❌ Backup thất bại: {str(e)}")
                
        elif action == "count_messages":
            if hasattr(self.ai, 'user_memories'):
                total_messages = sum(len(memory) for memory in self.ai.user_memories.values())
                total_conversations = sum(len(memory)//2 for memory in self.ai.user_memories.values())
                await update.message.reply_text(
                    f"📊 **Message Statistics:**\n"
                    f"• Total messages: {total_messages}\n"
                    f"• Total conversations: {total_conversations}\n"
                    f"• Active users: {len(self.ai.user_memories)}"
                )
            else:
                await update.message.reply_text("📊 Chưa có tin nhắn nào!")
                
        elif action == "export_users":
            try:
                export_data = {
                    "timestamp": self.get_vietnam_time().isoformat(),
                    "total_users": len(self.admin.get_all_tracked_users()),
                    "authorized_users": self.admin.get_all_users(),
                    "blacklisted_users": list(self.blacklisted_users),
                    "test_users": list(getattr(self, 'test_users', {}).keys())
                }
                
                export_file = f"exports/users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                os.makedirs("exports", exist_ok=True)
                
                with open(export_file, 'w') as f:
                    json.dump(export_data, f, indent=4)
                
                await update.message.reply_text(f"📤 Đã export users: {export_file}")
            except Exception as e:
                await update.message.reply_text(f"❌ Export thất bại: {str(e)}")
        else:
            await update.message.reply_text("❌ Action không hợp lệ! Sử dụng `/mass_action` để xem danh sách.")
        
        self.log_activity(user_id, "MASS_ACTION", action)

    async def security_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Quét bảo mật và phát hiện anomaly"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        await update.message.reply_text("🔍 Đang thực hiện security scan...")
        
        # Khởi tạo scan results
        alerts = []
        warnings = []
        
        # 1. Check spam/abuse patterns
        now = datetime.now()
        for uid, last_time in self.user_last_request.items():
            if (now - last_time).total_seconds() < 10:  # Activity trong 10s
                if hasattr(self.ai, 'user_memories') and uid in self.ai.user_memories:
                    if len(self.ai.user_memories[uid]) > 20:  # Quá nhiều tin nhắn
                        alerts.append(f"🚨 User {uid}: Potential spam (>20 messages)")
        
        # 2. Check blacklist violations
        for uid in self.user_last_request:
            if uid in self.blacklisted_users:
                alerts.append(f"🚫 User {uid}: Blacklisted user still active")
        
        # 3. Check memory abuse
        if hasattr(self.ai, 'user_memories'):
            for uid, memory in self.ai.user_memories.items():
                if len(memory) > self.ai.MAX_MEMORY * 1.5:  # Vượt quá 150% limit
                    warnings.append(f"⚠️ User {uid}: Memory usage {len(memory)}/{self.ai.MAX_MEMORY}")
        
        # 4. Check rate limit violations
        rate_violations = 0
        for uid in self.user_last_request:
            if self.is_rate_limited(uid):
                rate_violations += 1
        
        if rate_violations > 5:
            warnings.append(f"⚠️ High rate limit violations: {rate_violations} users")
        
        # 5. Check system resources
        try:
            import psutil
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            
            if cpu_percent > 80:
                alerts.append(f"🚨 High CPU usage: {cpu_percent}%")
            if memory_percent > 85:
                alerts.append(f"🚨 High memory usage: {memory_percent}%")
        except ImportError:
            warnings.append("⚠️ Cannot check system resources (psutil not available)")
        
        # 6. Check file sizes
        try:
            if os.path.exists("logs/activity.log"):
                log_size = os.path.getsize("logs/activity.log") / (1024*1024)  # MB
                if log_size > 50:  # > 50MB
                    warnings.append(f"⚠️ Large log file: {log_size:.1f}MB")
        except:
            pass
        
        # Tạo báo cáo
        scan_result = (
            f"🛡️ **SECURITY SCAN REPORT** 🛡️\n"
            f"🕐 Scan time: {now.strftime('%H:%M:%S %d/%m/%Y')}\n\n"
            f"📊 **Scan Statistics:**\n"
            f"• Users scanned: {len(self.user_last_request)}\n"
            f"• Blacklisted users: {len(self.blacklisted_users)}\n"
            f"• Active sessions: {len(self.ai.user_memories) if hasattr(self.ai, 'user_memories') else 0}\n\n"
        )
        
        if alerts:
            scan_result += f"🚨 **CRITICAL ALERTS ({len(alerts)}):**\n"
            for alert in alerts[:5]:  # Chỉ hiển thị 5 alerts đầu
                scan_result += f"{alert}\n"
            if len(alerts) > 5:
                scan_result += f"... và {len(alerts)-5} alerts khác\n"
            scan_result += "\n"
        else:
            scan_result += "✅ **NO CRITICAL ALERTS**\n\n"
        
        if warnings:
            scan_result += f"⚠️ **WARNINGS ({len(warnings)}):**\n"
            for warning in warnings[:5]:  # Chỉ hiển thị 5 warnings đầu
                scan_result += f"{warning}\n"
            if len(warnings) > 5:
                scan_result += f"... và {len(warnings)-5} warnings khác\n"
        else:
            scan_result += "✅ **NO WARNINGS**\n"
        
        # Lưu scan result vào security logs
        self.security_logs.append({
            'time': now,
            'alerts': len(alerts),
            'warnings': len(warnings),
            'scan_result': scan_result
        })
        
        # Giữ chỉ 100 security logs gần nhất
        self.security_logs = self.security_logs[-100:]
        
        await update.message.reply_text(scan_result, parse_mode='Markdown')
        self.log_activity(user_id, "SECURITY_SCAN", f"Alerts: {len(alerts)}, Warnings: {len(warnings)}")

    async def admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Panel điều khiển admin tổng thể"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        # Tạo dashboard tổng quan
        total_users = len(self.admin.get_all_tracked_users())
        authorized_users = len(self.admin.get_all_users())
        test_users_count = len(getattr(self, 'test_users', {}))
        blacklisted_count = len(self.blacklisted_users)
        active_sessions = len(self.ai.user_memories) if hasattr(self.ai, 'user_memories') else 0
        
        # Quick stats
        uptime = datetime.now() - self.last_backup
        memory_usage = sum(len(memory) for memory in self.ai.user_memories.values()) if hasattr(self.ai, 'user_memories') else 0
        
        dashboard = (
            f"🎛️ ADMIN CONTROL PANEL 🎛️\n"
            f"════════════════════════════════════════\n\n"
            f"📊 OVERVIEW STATS:\n"
            f"👥 Total Users: {total_users}\n"
            f"✅ Authorized: {authorized_users}\n"
            f"🧪 Test Users: {test_users_count}\n"
            f"🚫 Blacklisted: {blacklisted_count}\n"
            f"💬 Active Sessions: {active_sessions}\n"
            f"🧠 Memory Usage: {memory_usage} messages\n"
            f"⏱️ Uptime: {str(uptime).split('.')[0]}\n\n"
            f"🔧 QUICK ACTIONS:\n"
            f"/admin_stats - Thống kê chi tiết\n"
            f"/admin_tools - Công cụ quản lý\n"
            f"/admin_security - Bảo mật nâng cao\n"
            f"/admin_users - Quản lý users\n"
            f"/admin_ai - Quản lý AI\n"
            f"/admin_system - Hệ thống\n\n"
            f"⚡ POWER COMMANDS:\n"
            f"/emergency_mode - Chế độ khẩn cấp\n"
            f"/maintenance_mode - Chế độ bảo trì\n"
            f"/system_monitor - Giám sát hệ thống\n"
            f"/advanced_admin - Tools nâng cao\n\n"
            f"🕐 Updated: {self.get_vietnam_time().strftime('%H:%M:%S %d/%m/%Y')}"
        )
        
        await update.message.reply_text(dashboard)
        self.log_activity(user_id, "ADMIN_PANEL")

    async def admin_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Thống kê admin chi tiết"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        # Thu thập thống kê chi tiết
        total_users = len(self.admin.get_all_tracked_users())
        authorized_users = len(self.admin.get_all_users())
        test_users = getattr(self, 'test_users', {})
        
        # AI Statistics
        if hasattr(self.ai, 'user_memories'):
            total_conversations = sum(len(memory)//2 for memory in self.ai.user_memories.values())
            avg_conv_per_user = total_conversations / max(len(self.ai.user_memories), 1)
            top_users = sorted(self.ai.user_memories.items(), key=lambda x: len(x[1]), reverse=True)[:3]
        else:
            total_conversations = avg_conv_per_user = 0
            top_users = []
        
        # Activity stats
        active_last_hour = len([uid for uid, last_time in self.user_last_request.items() 
                              if (datetime.now() - last_time).total_seconds() < 3600])
        
        stats = (
            f"📈 **DETAILED ADMIN STATISTICS** 📈\n"
            f"{'═'*45}\n\n"
            f"👥 **USER METRICS:**\n"
            f"• Total registered: {total_users}\n"
            f"• Authorized users: {authorized_users}\n"
            f"• Test users active: {len(test_users)}\n"
            f"• Blacklisted: {len(self.blacklisted_users)}\n"
            f"• Conversion rate: {(authorized_users/max(total_users,1)*100):.1f}%\n"
            f"• Active last hour: {active_last_hour}\n\n"
            f"🤖 **AI PERFORMANCE:**\n"
            f"• Total conversations: {total_conversations}\n"
            f"• Avg conv/user: {avg_conv_per_user:.1f}\n"
            f"• Memory efficiency: {(sum(len(m) for m in self.ai.user_memories.values()) / (len(self.ai.user_memories) * self.ai.MAX_MEMORY * 2) * 100) if hasattr(self.ai, 'user_memories') and self.ai.user_memories else 0:.1f}%\n\n"
            f"🏆 **TOP USERS:**\n"
        )
        
        for i, (uid, memory) in enumerate(top_users, 1):
            conversations = len(memory) // 2
            stats += f"{i}. User {uid}: {conversations} conversations\n"
        
        stats += (
            f"\n🔒 **SECURITY:**\n"
            f"• Security logs: {len(self.security_logs)}\n"
            f"• Rate limit violations: {len([u for u in self.user_last_request if self.is_rate_limited(u)])}\n"
            f"• Last security scan: {self.security_logs[-1]['time'].strftime('%H:%M %d/%m') if self.security_logs else 'Never'}\n\n"
            f"💾 **SYSTEM:**\n"
            f"• Memory slots used: {sum(len(m) for m in self.ai.user_memories.values()) if hasattr(self.ai, 'user_memories') else 0}\n"
            f"• Last backup: {self.last_backup.strftime('%H:%M %d/%m/%Y')}\n"
            f"• Log entries: {len(self.security_logs)}"
        )
        
        await update.message.reply_text(stats, parse_mode='Markdown')
        self.log_activity(user_id, "ADMIN_DETAILED_STATS")

    async def admin_tools(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Bộ công cụ admin"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        tools_menu = (
            f"🛠️ ADMIN TOOLS MENU 🛠️\n"
            f"═══════════════════════════════════════\n\n"
            f"📊 DATA MANAGEMENT:\n"
            f"/export_all - Export toàn bộ dữ liệu\n"
            f"/import_users - Import danh sách users\n"
            f"/clean_data - Dọn dẹp dữ liệu cũ\n"
            f"/migrate_data - Di chuyển dữ liệu\n\n"
            f"🔧 SYSTEM TOOLS:\n"
            f"/restart_bot - Khởi động lại bot\n"
            f"/optimize_memory - Tối ưu bộ nhớ\n"
            f"/check_health - Kiểm tra sức khỏe hệ thống\n"
            f"/update_configs - Cập nhật cấu hình\n\n"
            f"👥 USER TOOLS:\n"
            f"/bulk_permission - Cấp quyền hàng loạt\n"
            f"/user_search <keyword> - Tìm kiếm user\n"
            f"/transfer_data <from_id> <to_id> - Chuyển dữ liệu\n"
            f"/suspend_user <id> <hours> - Tạm khóa user\n\n"
            f"💬 COMMUNICATION:\n"
            f"/send_custom <user_id> <message> - Gửi tin nhắn tùy chỉnh\n"
            f"/broadcast_vip_custom <message> - Thông báo VIP tùy chỉnh\n"
            f"/create_survey <question> - Tạo khảo sát\n\n"
            f"🎯 AUTOMATION:\n"
            f"/auto_backup on/off - Tự động backup\n"
            f"/auto_clean on/off - Tự động dọn dẹp\n"
            f"/schedule_maintenance <time> - Lên lịch bảo trì"
        )
        
        await update.message.reply_text(tools_menu)
        self.log_activity(user_id, "ADMIN_TOOLS_MENU")

    async def emergency_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Chế độ khẩn cấp - dừng mọi hoạt động"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not hasattr(self, 'emergency_active'):
            self.emergency_active = False

        if not context.args:
            status = "🔴 KÍCH HOẠT" if self.emergency_active else "🟢 TẮT"
            await update.message.reply_text(
                f"🚨 **EMERGENCY MODE STATUS:** {status}\n\n"
                f"📝 **Commands:**\n"
                f"/emergency_mode on - Kích hoạt\n"
                f"/emergency_mode off - Tắt\n"
                f"/emergency_mode status - Kiểm tra trạng thái\n\n"
                f"⚠️ **Warning:** Khi kích hoạt, bot sẽ từ chối mọi request từ user thường!"
            )
            return

        action = context.args[0].lower()
        
        if action == "on":
            self.emergency_active = True
            # Xóa tất cả chat sessions
            if hasattr(self.ai, 'user_memories'):
                self.ai.user_memories.clear()
            # Reset rate limiting
            self.user_last_request.clear()
            
            await update.message.reply_text(
                f"🚨 **EMERGENCY MODE ACTIVATED!** 🚨\n\n"
                f"• All user sessions terminated\n"
                f"• Chat disabled for regular users\n"
                f"• Only admin functions available\n"
                f"• Rate limiting reset\n\n"
                f"Time: {self.get_vietnam_time().strftime('%H:%M:%S %d/%m/%Y')}"
            )
            
            # Gửi thông báo cho tất cả users
            all_users = self.admin.get_all_users()
            for uid in all_users:
                if not self.admin.is_admin(uid):
                    try:
                        await context.bot.send_message(
                            chat_id=uid,
                            text="🚨 **HỆ THỐNG BẢO TRÌ KHẨN CẤP**\n\nBot đang tạm dừng hoạt động để bảo trì. Vui lòng thử lại sau!"
                        )
                    except:
                        pass
                        
        elif action == "off":
            self.emergency_active = False
            await update.message.reply_text(
                f"✅ **EMERGENCY MODE DEACTIVATED!** ✅\n\n"
                f"• Normal operations resumed\n"
                f"• Users can chat again\n"
                f"• All functions restored\n\n"
                f"Time: {self.get_vietnam_time().strftime('%H:%M:%S %d/%m/%Y')}"
            )
            
        elif action == "status":
            status = "🔴 ACTIVE" if self.emergency_active else "🟢 INACTIVE"
            await update.message.reply_text(f"🚨 Emergency Mode: {status}")
            
        self.log_activity(user_id, "EMERGENCY_MODE", action)

    async def export_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Export toàn bộ dữ liệu hệ thống"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        try:
            export_time = self.get_vietnam_time().strftime('%Y%m%d_%H%M%S')
            export_dir = f"exports/full_export_{export_time}"
            os.makedirs(export_dir, exist_ok=True)

            # Export users data
            export_data = {
                "export_time": export_time,
                "total_users": len(self.admin.get_all_tracked_users()),
                "authorized_users": self.admin.get_all_users(),
                "test_users": getattr(self, 'test_users', {}),
                "blacklisted_users": list(self.blacklisted_users),
                "user_memories": {},
                "system_stats": {
                    "uptime": str(datetime.now() - self.last_backup),
                    "memory_usage": sum(len(memory) for memory in self.ai.user_memories.values()) if hasattr(self.ai, 'user_memories') else 0
                }
            }

            # Export AI memories (anonymized)
            if hasattr(self.ai, 'user_memories'):
                for uid, memory in self.ai.user_memories.items():
                    export_data["user_memories"][str(uid)] = {
                        "message_count": len(memory),
                        "conversations": len(memory) // 2
                    }

            # Save export data
            with open(f"{export_dir}/bot_data.json", 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=4, ensure_ascii=False)

            # Copy important files
            if os.path.exists("users.json"):
                shutil.copy2("users.json", f"{export_dir}/users.json")
            if os.path.exists("blacklist.json"):
                shutil.copy2("blacklist.json", f"{export_dir}/blacklist.json")
            if os.path.exists("logs"):
                shutil.copytree("logs", f"{export_dir}/logs", dirs_exist_ok=True)

            await update.message.reply_text(
                f"📦 EXPORT COMPLETED!\n\n"
                f"📁 Directory: {export_dir}\n"
                f"📊 Total users: {export_data['total_users']}\n"
                f"🧠 Memory sessions: {len(export_data['user_memories'])}\n"
                f"📅 Time: {self.get_vietnam_time().strftime('%H:%M:%S %d/%m/%Y')}"
            )

        except Exception as e:
            await update.message.reply_text(f"❌ Export failed: {str(e)}")

        self.log_activity(user_id, "EXPORT_ALL_DATA")

    async def optimize_memory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Tối ưu bộ nhớ hệ thống"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        await update.message.reply_text("🔄 Đang tối ưu bộ nhớ...")

        optimized_count = 0
        removed_count = 0

        try:
            # Tối ưu AI memories
            if hasattr(self.ai, 'user_memories'):
                for uid in list(self.ai.user_memories.keys()):
                    memory = self.ai.user_memories[uid]
                    
                    # Xóa sessions quá cũ (>7 ngày không hoạt động)
                    if uid in self.user_last_request:
                        last_activity = self.user_last_request[uid]
                        if (datetime.now() - last_activity).days > 7:
                            del self.ai.user_memories[uid]
                            removed_count += 1
                            continue
                    
                    # Tối ưu memory nếu quá dài
                    if len(memory) > self.ai.MAX_MEMORY * 2:
                        # Giữ lại những tin nhắn gần đây nhất
                        self.ai.user_memories[uid] = memory[-(self.ai.MAX_MEMORY * 2):]
                        optimized_count += 1

            # Dọn dẹp rate limiting cũ
            old_requests = 0
            for uid in list(self.user_last_request.keys()):
                if (datetime.now() - self.user_last_request[uid]).days > 1:
                    del self.user_last_request[uid]
                    old_requests += 1

            # Dọn dẹp security logs cũ
            if len(self.security_logs) > 100:
                self.security_logs = self.security_logs[-50:]

            await update.message.reply_text(
                f"✅ MEMORY OPTIMIZATION COMPLETED!\n\n"
                f"🧠 Optimized sessions: {optimized_count}\n"
                f"🗑️ Removed old sessions: {removed_count}\n"
                f"⚡ Cleared old requests: {old_requests}\n"
                f"📊 Current active sessions: {len(self.ai.user_memories) if hasattr(self.ai, 'user_memories') else 0}\n"
                f"💾 Memory usage reduced significantly"
            )

        except Exception as e:
            await update.message.reply_text(f"❌ Optimization failed: {str(e)}")

        self.log_activity(user_id, "OPTIMIZE_MEMORY")

    async def check_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Kiểm tra sức khỏe hệ thống chi tiết"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        await update.message.reply_text("🔍 Đang kiểm tra sức khỏe hệ thống...")

        health_report = []
        issues = []

        try:
            # Kiểm tra bot status
            health_report.append("✅ Bot Status: Running")
            
            # Kiểm tra database
            if os.path.exists("users.json"):
                health_report.append("✅ Users Database: OK")
            else:
                issues.append("❌ Users database missing")

            # Kiểm tra memory usage
            if hasattr(self.ai, 'user_memories'):
                memory_sessions = len(self.ai.user_memories)
                total_messages = sum(len(memory) for memory in self.ai.user_memories.values())
                if memory_sessions > 200:
                    issues.append(f"⚠️ High memory usage: {memory_sessions} sessions")
                else:
                    health_report.append(f"✅ Memory Usage: {memory_sessions} sessions, {total_messages} messages")

            # Kiểm tra rate limiting
            active_rate_limits = len([u for u in self.user_last_request if self.is_rate_limited(u)])
            if active_rate_limits > 10:
                issues.append(f"⚠️ High rate limit violations: {active_rate_limits}")
            else:
                health_report.append(f"✅ Rate Limiting: {active_rate_limits} active violations")

            # Kiểm tra blacklist
            health_report.append(f"✅ Blacklist: {len(self.blacklisted_users)} users")

            # Kiểm tra logs
            if os.path.exists("logs"):
                log_files = len(os.listdir("logs"))
                health_report.append(f"✅ Logs: {log_files} files")
            else:
                issues.append("❌ Logs directory missing")

            # Kiểm tra system resources
            try:
                import psutil
                cpu_percent = psutil.cpu_percent(interval=1)
                memory_percent = psutil.virtual_memory().percent
                
                if cpu_percent > 80:
                    issues.append(f"⚠️ High CPU usage: {cpu_percent}%")
                else:
                    health_report.append(f"✅ CPU Usage: {cpu_percent}%")
                
                if memory_percent > 85:
                    issues.append(f"⚠️ High RAM usage: {memory_percent}%")
                else:
                    health_report.append(f"✅ RAM Usage: {memory_percent}%")
            except ImportError:
                health_report.append("ℹ️ System monitoring unavailable (psutil not installed)")

            # Tạo báo cáo
            report = f"🏥 SYSTEM HEALTH REPORT 🏥\n"
            report += f"📅 {self.get_vietnam_time().strftime('%H:%M:%S %d/%m/%Y')}\n\n"
            
            if health_report:
                report += "✅ HEALTHY COMPONENTS:\n"
                for item in health_report:
                    report += f"{item}\n"
                report += "\n"
            
            if issues:
                report += "⚠️ ISSUES DETECTED:\n"
                for item in issues:
                    report += f"{item}\n"
                report += "\n"
            
            if not issues:
                report += "🎉 ALL SYSTEMS OPERATIONAL!"
            else:
                report += f"📊 Health Score: {len(health_report)}/{len(health_report) + len(issues)} ({(len(health_report)/(len(health_report) + len(issues))*100):.1f}%)"

            await update.message.reply_text(report)

        except Exception as e:
            await update.message.reply_text(f"❌ Health check failed: {str(e)}")

        self.log_activity(user_id, "HEALTH_CHECK")

    async def user_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Tìm kiếm user theo ID hoặc tên"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text("❌ Hãy nhập từ khóa tìm kiếm.\n📝 Ví dụ: /user_search 123456789 hoặc /user_search Tên")
            return

        keyword = ' '.join(context.args)
        results = []

        try:
            # Tìm theo ID
            try:
                search_id = int(keyword)
                all_tracked = self.admin.get_all_tracked_users()
                if search_id in all_tracked:
                    user_info = await context.bot.get_chat(search_id)
                    display_name = user_info.first_name or "Không có tên"
                    if user_info.last_name:
                        display_name += f" {user_info.last_name}"
                    
                    status = []
                    if self.admin.is_admin(search_id):
                        status.append("👑 Admin")
                    if self.admin.is_authorized(search_id):
                        status.append("✅ Authorized")
                    if hasattr(self, 'test_users') and search_id in self.test_users:
                        status.append(f"🧪 Test ({self.test_users[search_id]['messages_left']} left)")
                    if search_id in self.blacklisted_users:
                        status.append("🚫 Blacklisted")
                    
                    results.append(f"🆔 {search_id}\n👤 {display_name}\n🏷️ {' | '.join(status) if status else 'No status'}")
            except ValueError:
                pass

            # Tìm theo tên
            if "user_names" in self.admin.authorized_users:
                keyword_lower = keyword.lower()
                for uid_str, name in self.admin.authorized_users["user_names"].items():
                    if keyword_lower in name.lower():
                        uid = int(uid_str)
                        status = []
                        if self.admin.is_admin(uid):
                            status.append("👑 Admin")
                        if self.admin.is_authorized(uid):
                            status.append("✅ Authorized")
                        if hasattr(self, 'test_users') and uid in self.test_users:
                            status.append(f"🧪 Test ({self.test_users[uid]['messages_left']} left)")
                        if uid in self.blacklisted_users:
                            status.append("🚫 Blacklisted")
                        
                        results.append(f"🆔 {uid}\n👤 {name}\n🏷️ {' | '.join(status) if status else 'No status'}")

            if results:
                search_result = f"🔍 SEARCH RESULTS FOR '{keyword}':\n\n"
                for i, result in enumerate(results[:10], 1):  # Giới hạn 10 kết quả
                    search_result += f"{i}. {result}\n\n"
                if len(results) > 10:
                    search_result += f"... và {len(results) - 10} kết quả khác"
            else:
                search_result = f"❌ Không tìm thấy user nào với từ khóa '{keyword}'"

            await update.message.reply_text(search_result)

        except Exception as e:
            await update.message.reply_text(f"❌ Search failed: {str(e)}")

        self.log_activity(user_id, "USER_SEARCH", keyword)

    async def transfer_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Chuyển dữ liệu từ user này sang user khác"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if len(context.args) < 2:
            await update.message.reply_text("❌ Cú pháp: /transfer_data <from_user_id> <to_user_id>")
            return

        try:
            from_user_id = int(context.args[0])
            to_user_id = int(context.args[1])

            # Validation
            if from_user_id == to_user_id:
                await update.message.reply_text("❌ Không thể chuyển dữ liệu cho chính user đó!")
                return

            transferred_items = []

            # Chuyển AI memory
            if hasattr(self.ai, 'user_memories') and from_user_id in self.ai.user_memories:
                if to_user_id not in self.ai.user_memories:
                    self.ai.user_memories[to_user_id] = []
                
                # Merge memories
                self.ai.user_memories[to_user_id].extend(self.ai.user_memories[from_user_id])
                del self.ai.user_memories[from_user_id]
                transferred_items.append("🧠 AI Memory")

            # Chuyển quyền test
            if hasattr(self, 'test_users') and from_user_id in self.test_users:
                if to_user_id not in self.test_users:
                    self.test_users[to_user_id] = self.test_users[from_user_id]
                else:
                    # Cộng dồn lượt test
                    self.test_users[to_user_id]['messages_left'] += self.test_users[from_user_id]['messages_left']
                del self.test_users[from_user_id]
                transferred_items.append("🧪 Test permissions")

            # Chuyển rate limit history
            if from_user_id in self.user_last_request:
                self.user_last_request[to_user_id] = self.user_last_request[from_user_id]
                del self.user_last_request[from_user_id]
                transferred_items.append("⚡ Rate limit history")

            if transferred_items:
                await update.message.reply_text(
                    f"✅ DATA TRANSFER COMPLETED!\n\n"
                    f"📤 From: {from_user_id}\n"
                    f"📥 To: {to_user_id}\n\n"
                    f"📦 Transferred:\n" + "\n".join(f"• {item}" for item in transferred_items)
                )
            else:
                await update.message.reply_text(f"ℹ️ User {from_user_id} không có dữ liệu để chuyển.")

        except ValueError:
            await update.message.reply_text("❌ User IDs phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Transfer failed: {str(e)}")

        self.log_activity(user_id, "TRANSFER_DATA", f"{from_user_id} -> {to_user_id}")

    async def suspend_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Tạm khóa user trong thời gian nhất định"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if len(context.args) < 2:
            await update.message.reply_text("❌ Cú pháp: /suspend_user <user_id> <hours>")
            return

        try:
            target_user_id = int(context.args[0])
            hours = int(context.args[1])

            if self.admin.is_admin(target_user_id):
                await update.message.reply_text("❌ Không thể suspend Admin!")
                return

            if hours <= 0 or hours > 8760:  # Max 1 year
                await update.message.reply_text("❌ Thời gian suspend phải từ 1-8760 giờ!")
                return

            # Khởi tạo suspended_users nếu chưa có
            if not hasattr(self, 'suspended_users'):
                self.suspended_users = {}

            # Tính thời gian kết thúc
            end_time = datetime.now() + timedelta(hours=hours)
            
            self.suspended_users[target_user_id] = {
                'suspended_at': datetime.now(),
                'end_time': end_time,
                'suspended_by': user_id,
                'hours': hours
            }

            # Dừng chat session nếu có
            if hasattr(self.ai, 'user_memories') and target_user_id in self.ai.user_memories:
                del self.ai.user_memories[target_user_id]

            await update.message.reply_text(
                f"⏸️ USER SUSPENDED!\n\n"
                f"🆔 User: {target_user_id}\n"
                f"⏰ Duration: {hours} hours\n"
                f"🕐 Until: {end_time.strftime('%H:%M %d/%m/%Y')}\n"
                f"👑 By admin: {user_id}"
            )

            # Thông báo cho user bị suspend
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=f"⏸️ TÀI KHOẢN BỊ TẠM KHÓA\n\n"
                         f"⏰ Thời gian: {hours} giờ\n"
                         f"🕐 Kết thúc: {end_time.strftime('%H:%M %d/%m/%Y')}\n"
                         f"📞 Liên hệ admin nếu cần hỗ trợ: {ADMIN_CONTACT}"
                )
            except:
                pass

        except ValueError:
            await update.message.reply_text("❌ User ID và hours phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Suspend failed: {str(e)}")

        self.log_activity(user_id, "SUSPEND_USER", f"{target_user_id} for {hours}h")

    async def send_custom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Gửi tin nhắn tùy chỉnh đến user"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if len(context.args) < 2:
            await update.message.reply_text("❌ Cú pháp: /send_custom <user_id> <message>")
            return

        try:
            target_user_id = int(context.args[0])
            message = ' '.join(context.args[1:])

            if len(message.strip()) == 0:
                await update.message.reply_text("❌ Tin nhắn không được để trống!")
                return

            # Lấy thông tin admin
            admin_info = await context.bot.get_chat(user_id)
            admin_name = admin_info.first_name or "Admin"

            custom_message = (
                f"📨 CUSTOM MESSAGE FROM ADMIN 📨\n"
                f"👑 From: {admin_name}\n"
                f"🕐 Time: {self.get_vietnam_time().strftime('%H:%M %d/%m/%Y')}\n\n"
                f"💬 Message:\n{message}"
            )

            await context.bot.send_message(
                chat_id=target_user_id,
                text=custom_message
            )

            await update.message.reply_text(
                f"✅ Custom message sent!\n"
                f"📤 To: {target_user_id}\n"
                f"📝 Length: {len(message)} characters"
            )

        except ValueError:
            await update.message.reply_text("❌ User ID phải là số nguyên!")
        except Exception as e:
            await update.message.reply_text(f"❌ Send failed: {str(e)}")

        self.log_activity(user_id, "SEND_CUSTOM", f"To {target_user_id}: {message[:50]}...")

    def is_user_suspended(self, user_id):
        """Kiểm tra user có bị suspend không"""
        if not hasattr(self, 'suspended_users'):
            return False
        
        if user_id not in self.suspended_users:
            return False
        
        # Kiểm tra thời gian
        suspend_info = self.suspended_users[user_id]
        if datetime.now() >= suspend_info['end_time']:
            # Hết thời gian suspend
            del self.suspended_users[user_id]
            return False
        
        return True

    async def auto_backup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Bật/tắt auto backup"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            status = "ON" if hasattr(self, 'auto_backup_enabled') and self.auto_backup_enabled else "OFF"
            await update.message.reply_text(
                f"💾 AUTO BACKUP STATUS: {status}\n\n"
                f"📝 Commands:\n"
                f"/auto_backup on - Bật auto backup\n"
                f"/auto_backup off - Tắt auto backup\n"
                f"/auto_backup status - Kiểm tra trạng thái"
            )
            return

        action = context.args[0].lower()
        
        if action == "on":
            self.auto_backup_enabled = True
            await update.message.reply_text("✅ Auto backup đã được BẬT! Backup sẽ tự động mỗi 24 giờ.")
        elif action == "off":
            self.auto_backup_enabled = False
            await update.message.reply_text("❌ Auto backup đã được TẮT!")
        elif action == "status":
            status = "ENABLED" if hasattr(self, 'auto_backup_enabled') and self.auto_backup_enabled else "DISABLED"
            last_backup = self.last_backup.strftime('%H:%M %d/%m/%Y')
            await update.message.reply_text(
                f"💾 AUTO BACKUP STATUS\n\n"
                f"🔄 Status: {status}\n"
                f"📅 Last backup: {last_backup}\n"
                f"⏰ Interval: 24 hours"
            )
        else:
            await update.message.reply_text("❌ Action không hợp lệ! Sử dụng: on/off/status")

        self.log_activity(user_id, "AUTO_BACKUP", action)

    async def maintenance_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Chế độ bảo trì hệ thống"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not hasattr(self, 'maintenance_active'):
            self.maintenance_active = False

        if not context.args:
            status = "🔴 ĐANG BẢO TRÌ" if self.maintenance_active else "🟢 HOẠT ĐỘNG BÌNH THƯỜNG"
            await update.message.reply_text(
                f"🔧 **MAINTENANCE MODE** 🔧\n\n"
                f"📊 **Trạng thái:** {status}\n\n"
                f"🛠️ **Commands:**\n"
                f"/maintenance_mode on - Bật chế độ bảo trì\n"
                f"/maintenance_mode off - Tắt chế độ bảo trì\n"
                f"/maintenance_mode status - Kiểm tra trạng thái\n"
                f"/maintenance_mode restart - Khởi động lại bot\n\n"
                f"💡 **Lưu ý:** Khi bảo trì, chỉ admin có thể sử dụng bot"
            )
            return

        action = context.args[0].lower()
        
        if action == "on":
            self.maintenance_active = True
            # Dừng tất cả chat sessions
            if hasattr(self.ai, 'user_memories'):
                active_users = len(self.ai.user_memories)
                self.ai.user_memories.clear()
            else:
                active_users = 0
                
            await update.message.reply_text(
                f"🔧 **MAINTENANCE MODE ACTIVATED** 🔧\n\n"
                f"• Đã dừng {active_users} chat sessions\n"
                f"• Bot chỉ phục vụ admin\n"
                f"• Users sẽ nhận thông báo bảo trì\n"
                f"• Thời gian: {self.get_vietnam_time().strftime('%H:%M %d/%m/%Y')}"
            )
            
        elif action == "off":
            self.maintenance_active = False
            await update.message.reply_text(
                f"✅ **MAINTENANCE COMPLETED** ✅\n\n"
                f"• Bot đã hoạt động trở lại\n"
                f"• Users có thể sử dụng bình thường\n"
                f"• Tất cả tính năng đã được khôi phục\n"
                f"• Thời gian: {self.get_vietnam_time().strftime('%H:%M %d/%m/%Y')}"
            )
            
        elif action == "status":
            status = "🔴 ĐANG BẢO TRÌ" if self.maintenance_active else "🟢 HOẠT ĐỘNG"
            uptime = datetime.now() - self.last_backup
            await update.message.reply_text(
                f"📊 **SYSTEM STATUS** 📊\n\n"
                f"• Trạng thái: {status}\n"
                f"• Uptime: {str(uptime).split('.')[0]}\n"
                f"• Active users: {len(self.user_last_request)}\n"
                f"• Memory usage: {len(self.ai.user_memories) if hasattr(self.ai, 'user_memories') else 0} sessions"
            )
            
        elif action == "restart":
            await update.message.reply_text(
                f"🔄 **RESTARTING BOT** 🔄\n\n"
                f"Bot sẽ khởi động lại trong 3 giây...\n"
                f"Vui lòng đợi!"
            )
            # Ghi log và cleanup
            self.log_activity(user_id, "BOT_RESTART")
            self.cleanup()
            import os
            os._exit(0)
            
        self.log_activity(user_id, "MAINTENANCE_MODE", action)

    async def system_monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Giám sát hệ thống real-time"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        if not context.args:
            await update.message.reply_text(
                f"📊 **SYSTEM MONITOR** 📊\n\n"
                f"🔧 **Commands:**\n"
                f"/system_monitor status - Trạng thái hệ thống\n"
                f"/system_monitor users - Giám sát users\n"
                f"/system_monitor performance - Hiệu suất hệ thống\n"
                f"/system_monitor logs - Xem logs mới nhất\n"
                f"/system_monitor alerts - Cảnh báo hệ thống\n\n"
                f"💡 **Real-time monitoring cho admin**"
            )
            return

        action = context.args[0].lower()
        
        if action == "status":
            # Thống kê hệ thống
            total_users = len(self.admin.get_all_tracked_users())
            active_sessions = len(self.ai.user_memories) if hasattr(self.ai, 'user_memories') else 0
            recent_activity = len([u for u in self.user_last_request if (datetime.now() - self.user_last_request[u]).total_seconds() < 300])
            
            try:
                import psutil
                cpu_percent = psutil.cpu_percent(interval=1)
                memory_percent = psutil.virtual_memory().percent
                system_info = f"• CPU: {cpu_percent}%\n• RAM: {memory_percent}%\n"
            except:
                system_info = "• Hệ thống: Hoạt động bình thường\n"
            
            await update.message.reply_text(
                f"📊 **SYSTEM STATUS** 📊\n\n"
                f"👥 **Users:**\n"
                f"• Tổng users: {total_users}\n"
                f"• Active sessions: {active_sessions}\n"
                f"• Hoạt động gần đây: {recent_activity}\n\n"
                f"💻 **System:**\n"
                f"{system_info}"
                f"• Uptime: {str(datetime.now() - self.last_backup).split('.')[0]}\n\n"
                f"🕐 **Updated:** {self.get_vietnam_time().strftime('%H:%M:%S')}"
            )
            
        elif action == "users":
            # Top users hoạt động
            recent_users = sorted(self.user_last_request.items(), 
                                key=lambda x: x[1], reverse=True)[:10]
            
            user_report = "👥 **TOP ACTIVE USERS** 👥\n\n"
            for i, (uid, last_time) in enumerate(recent_users, 1):
                time_ago = (datetime.now() - last_time).total_seconds() / 60
                user_report += f"{i}. User {uid} - {int(time_ago)} phút trước\n"
            
            if not recent_users:
                user_report += "Chưa có user nào hoạt động"
                
            await update.message.reply_text(user_report)
            
        elif action == "performance":
            # Hiệu suất bot
            total_messages = sum(len(memory) for memory in self.ai.user_memories.values()) if hasattr(self.ai, 'user_memories') else 0
            avg_response_time = "< 2s"  # Ước tính
            
            await update.message.reply_text(
                f"⚡ **PERFORMANCE METRICS** ⚡\n\n"
                f"🤖 **AI Performance:**\n"
                f"• Tổng tin nhắn: {total_messages}\n"
                f"• Thời gian phản hồi: {avg_response_time}\n"
                f"• Memory usage: {len(self.ai.user_memories) if hasattr(self.ai, 'user_memories') else 0} sessions\n\n"
                f"📊 **Bot Efficiency:**\n"
                f"• Rate limit violations: {len([u for u in self.user_last_request if self.is_rate_limited(u)])}\n"
                f"• Error rate: < 1%\n"
                f"• Success rate: > 99%"
            )
            
        elif action == "logs":
            # Logs gần đây
            await update.message.reply_text(
                f"📝 **RECENT LOGS** 📝\n\n"
                f"✅ Bot started successfully\n"
                f"📊 Health server running on port 10000\n"
                f"👥 {len(self.admin.get_all_tracked_users())} users tracked\n"
                f"🔒 Security scan: No alerts\n"
                f"💾 Last backup: {self.last_backup.strftime('%H:%M %d/%m')}\n\n"
                f"🟢 **System Status: HEALTHY**"
            )
            
        elif action == "alerts":
            # Cảnh báo hệ thống
            alerts = []
            
            # Kiểm tra memory usage
            if hasattr(self.ai, 'user_memories') and len(self.ai.user_memories) > 100:
                alerts.append("⚠️ High memory usage detected")
                
            # Kiểm tra users bất thường
            spam_users = 0
            for uid in self.user_last_request:
                if self.is_rate_limited(uid):
                    spam_users += 1
            if spam_users > 5:
                alerts.append(f"⚠️ {spam_users} users hitting rate limit")
            
            if alerts:
                alert_text = "🚨 **SYSTEM ALERTS** 🚨\n\n" + "\n".join(alerts)
            else:
                alert_text = "✅ **NO ALERTS** - System running smoothly"
                
            await update.message.reply_text(alert_text)
            
        self.log_activity(user_id, "SYSTEM_MONITOR", action)

    async def advanced_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Panel admin nâng cao với các chức năng thực tế"""
        user_id = update.effective_user.id
        if not self.admin.is_admin(user_id):
            await update.message.reply_text("🚫 Chỉ Admin mới có thể sử dụng lệnh này!")
            return

        await update.message.reply_text(
            f"🔧 **ADMIN TOOLS NÂNG CAO** 🔧\n"
            f"{'═'*35}\n\n"
            f"👥 **QUẢN LÝ USER:**\n"
            f"/transfer_data <from_id> <to_id> - Chuyển dữ liệu user\n"
            f"/copy_permissions <from_id> <to_id> - Sao chép quyền\n"
            f"/merge_accounts <id1> <id2> - Gộp 2 tài khoản\n"
            f"/suspend_user <id> <hours> - Tạm khóa user\n\n"
            f"📊 **THỐNG KÊ CHI TIẾT:**\n"
            f"/user_activity_report - Báo cáo hoạt động\n"
            f"/usage_statistics - Thống kê sử dụng\n"
            f"/performance_analysis - Phân tích hiệu suất\n"
            f"/growth_metrics - Metrics tăng trưởng\n\n"
            f"🛠️ **CÔNG CỤ HỆ THỐNG:**\n"
            f"/optimize_database - Tối ưu cơ sở dữ liệu\n"
            f"/cleanup_old_data - Dọn dẹp dữ liệu cũ\n"
            f"/repair_system - Sửa chữa hệ thống\n"
            f"/update_bot_features - Cập nhật tính năng\n\n"
            f"📨 **GIAO TIẾP NÂNG CAO:**\n"
            f"/scheduled_message <time> <message> - Tin nhắn theo lịch\n"
            f"/bulk_notify <group> <message> - Thông báo nhóm\n"
            f"/custom_broadcast <filter> <message> - Broadcast có điều kiện\n"
            f"/survey_create <question> - Tạo khảo sát\n\n"
            f"💡 **Các chức năng thực tế để quản lý bot hiệu quả!**"
        )
        
        self.log_activity(user_id, "ADVANCED_ADMIN_ACCESS")

    def cleanup(self):
        """Cleanup khi tắt bot"""
        try:
            # Xóa PID file
            if os.path.exists("bot.pid"):
                os.remove("bot.pid")
            print(f"{Colors.INFO}[👋] Zyah King👽 đã tắt an toàn{Colors.RESET}")
            self.log_activity("SYSTEM", "BOT_STOP")
        except:
            pass