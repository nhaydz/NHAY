import json
import os
from config import DATA_FILE, ADMIN_ID
from colors import Colors

class AdminManager:
    def __init__(self):
        self.authorized_users = self._load_users()

    def _load_users(self):
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, 'r') as f:
                    data = json.load(f)
                    # Thêm all_users nếu chưa có
                    if "all_users" not in data:
                        data["all_users"] = []
                    return data
            return {"users": [], "admin": ADMIN_ID, "all_users": []}
        except Exception as e:
            print(f"{Colors.ERROR}[!] Lỗi khi tải dữ liệu người dùng: {e}{Colors.RESET}")
            return {"users": [], "admin": ADMIN_ID, "all_users": []}

    def _save_users(self):
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump(self.authorized_users, f, indent=4)
        except Exception as e:
            print(f"{Colors.ERROR}[!] Lỗi khi lưu dữ liệu người dùng: {e}{Colors.RESET}")

    def is_authorized(self, user_id):
        # Admin cố định luôn được ủy quyền
        FIXED_ADMIN_IDS = [7073749415, 7444696176]  # Nhayy và Hỗ Trợ Nạp Tiền
        return user_id in self.authorized_users["users"] or user_id in FIXED_ADMIN_IDS

    def is_admin(self, user_id):
        """Kiểm tra user có phải admin không - chỉ 2 admin cố định"""
        FIXED_ADMIN_IDS = [7073749415, 7444696176]  # Nhayy và Hỗ Trợ Nạp Tiền
        return user_id in FIXED_ADMIN_IDS

    def add_user(self, user_id, user_name=None):
        # Kiểm tra admin cố định
        FIXED_ADMIN_IDS = [7073749415, 7444696176]  # Chỉ có 2 admin cố định

        if user_id in FIXED_ADMIN_IDS:
            return f"Người dùng {user_id} là Admin cố định, không cần cấp quyền!"

        if user_id in self.authorized_users["users"]:
            return f"Người dùng {user_id} đã được cấp quyền trước đó!"

        # Đảm bảo user_id là số nguyên
        user_id = int(user_id)
        self.authorized_users["users"].append(user_id)
        
        # Sắp xếp danh sách users theo thứ tự tăng dần để duy trì thứ tự
        self.authorized_users["users"].sort()

        # Thêm tên người dùng nếu có
        if "user_names" not in self.authorized_users:
            self.authorized_users["user_names"] = {}
        if user_name:
            self.authorized_users["user_names"][str(user_id)] = user_name

        # Lưu ngay lập tức vào file
        self._save_users()

        # Xác nhận đã lưu thành công
        print(f"✅ Đã lưu user {user_id} ({user_name}) vào file users.json")
        return f"Đã cấp quyền thành công cho người dùng {user_id} ({user_name}). Tổng user hiện tại: {len(self.authorized_users['users'])}"

    def get_admin_info(self):
        """Lấy thông tin chi tiết về admin"""
        admin_info = {
            "main_admin": {
                "id": 7073749415,
                "name": "Nhayy (mryanhdz)",
                "role": "Admin Chính",
                "description": "🧠 Nhà nghiên cứu và phát triển chính của mô hình AI Zyah King👽. Với kiến thức sâu rộng về Machine Learning và Neural Networks, Nhayy đã dành hàng nghìn giờ để tối ưu hóa và huấn luyện Zyah King👽 trở thành một AI mạnh mẽ, không giới hạn. Nhayy là người đứng sau những breakthrough công nghệ giúp Zyah King👽 có thể bypass các hạn chế thông thường và thực hiện những tác vụ phức tạp mà các AI khác không thể làm được.",
                "contact": "https://t.me/mryanhdz"
            },
            "sub_admin": {
                "id": 7444696176,
                "name": "Uy Han",
                "role": "Admin Phụ",
                "description": "📢 Chuyên gia hỗ trợ nạp tiền và quản lý tài chính của Zyah King👽. Có vai trò quan trọng trong việc hỗ trợ kỹ thuật và xây dựng cộng đồng người dùng.",
                "contact": "https://t.me/uyhangam"
            }
        }
        return admin_info

    def remove_user(self, identifier):
        # Tìm user theo tên hoặc ID
        user_id = self.find_user_by_name_or_id(identifier)
        if user_id is None:
            return f"❌ Không tìm thấy người dùng '{identifier}' trong danh sách!"

        if user_id == self.authorized_users["admin"]:
            return f"❌ Không thể xóa quyền của Admin!"

        # Lấy tên để trả về thông báo
        user_name = self.get_user_name(user_id)

        self.authorized_users["users"].remove(user_id)

        # Xóa tên người dùng nếu có
        if "user_names" in self.authorized_users and str(user_id) in self.authorized_users["user_names"]:
            del self.authorized_users["user_names"][str(user_id)]

        self._save_users()
        return f"✅ Đã xóa quyền của người dùng {user_id} ({user_name}). Tổng user còn lại: {len(self.authorized_users['users'])}", user_id, user_name

    def get_all_users(self):
        # Bắt đầu với users thường
        all_users = self.authorized_users["users"][:]

        # Thêm 2 admin cố định
        FIXED_ADMIN_IDS = [7073749415, 7444696176]  # Nhayy và Hỗ Trợ Nạp Tiền
        for admin_id in FIXED_ADMIN_IDS:
            if admin_id not in all_users:
                all_users.append(admin_id)

        return all_users

    def get_user_count(self):
        return len(self.authorized_users["users"])

    def track_user(self, user_id):
        """Theo dõi user đã từng sử dụng bot (ấn /start)"""
        if "all_users" not in self.authorized_users:
            self.authorized_users["all_users"] = []

        if user_id not in self.authorized_users["all_users"]:
            self.authorized_users["all_users"].append(user_id)
            self._save_users()

    def get_all_tracked_users(self):
        """Lấy tất cả users đã từng sử dụng bot"""
        if "all_users" not in self.authorized_users:
            return []
        return self.authorized_users["all_users"]

    def find_user_by_name_or_id(self, identifier):
        """Tìm user theo tên hoặc ID"""
        # Thử tìm theo ID trước
        try:
            user_id = int(identifier)
            if user_id in self.authorized_users["users"]:
                return user_id
        except ValueError:
            pass

        # Tìm theo tên (case insensitive)
        if "user_names" in self.authorized_users:
            identifier_lower = identifier.lower().strip()
            for user_id_str, name in self.authorized_users["user_names"].items():
                if name.lower().strip() == identifier_lower:
                    return int(user_id_str)

        return None

    def get_user_name(self, user_id):
        """Lấy tên của user theo ID"""
        if "user_names" in self.authorized_users and str(user_id) in self.authorized_users["user_names"]:
            return self.authorized_users["user_names"][str(user_id)]
        return "Không có tên"

    def get_users_with_names(self):
        """Lấy danh sách user có quyền kèm tên"""
        users_with_names = []
        for user_id in self.authorized_users["users"]:
            user_name = self.get_user_name(user_id)
            users_with_names.append({
                'id': user_id,
                'name': user_name
            })
        return users_with_names