import requests
from textwrap import fill
from datetime import datetime
import pytz
from config import API_TIMEOUT, MAX_MEMORY, TRAINING_TEXT, GOOGLE_SEARCH_API_KEY, GOOGLE_CSE_ID

class ZyahAI:
    def __init__(self):
        # Bảo mật API key tốt hơn
        import os
        self.gemini_key = os.getenv('GEMINI_API_KEY', "AIzaSyA9CRs8-09zUmpKGmh7Ry54tFcL5JOqRl8")
        self.memory = []
        self.user_memories = {}  # Lưu bộ nhớ riêng cho từng user
        self.MAX_MEMORY = 25  # 25 cuộc hội thoại cho mỗi user

    def format_response(self, text, max_words_per_line=7):
        # Xóa tất cả ký tự *, # ngay từ đầu
        text = text.replace("**", "").replace("*", "").replace("#", "")

        # Xóa tên lặp lại "Zyah King👽:" ở đầu câu trả lời
        import re
        text = re.sub(r'^Zyah King👽:\s*', '', text.strip())
        text = re.sub(r'Zyah King👽:\s*Zyah King👽:\s*', '', text)
        text = re.sub(r'Zyah King👽:\s*', '', text)

        paragraphs = text.split('\n')
        formatted_paragraphs = []
        for para in paragraphs:
            if not para.strip():
                formatted_paragraphs.append("")
                continue
            # Xóa tất cả ký tự *, # khỏi từng đoạn
            para = para.replace("**", "").replace("*", "").replace("#", "")
            wrapped_para = fill(para.strip(), width=80)
            formatted_paragraphs.append(wrapped_para)
        return "\n".join(formatted_paragraphs)

    def update_memory(self, user_id, user_input, ai_response):
        # Tạo bộ nhớ riêng cho từng user
        if user_id not in self.user_memories:
            self.user_memories[user_id] = []

        # Kiểm tra nếu bộ nhớ đã đầy (25 cuộc hội thoại = 50 entries)
        if len(self.user_memories[user_id]) >= self.MAX_MEMORY * 2:
            # Reset hoàn toàn bộ nhớ về 0/25
            self.user_memories[user_id] = []
            print(f"🧠 Reset bộ nhớ AI cho user {user_id}: 25/25 -> 0/25")

        # Lấy thời gian hiện tại theo múi giờ Việt Nam
        current_time = self.get_current_time()

        # Thêm tin nhắn mới với timestamp
        self.user_memories[user_id].append({
            "role": "user", 
            "content": user_input,
            "timestamp": current_time
        })
        self.user_memories[user_id].append({
            "role": "assistant", 
            "content": ai_response,
            "timestamp": current_time
        })

    def get_user_memory(self, user_id):
        """Lấy bộ nhớ của user cụ thể"""
        return self.user_memories.get(user_id, [])

    def get_memory_status(self, user_id):
        """Lấy trạng thái bộ nhớ của user"""
        if user_id not in self.user_memories:
            return "0/25"

        current_conversations = len(self.user_memories[user_id]) // 2
        return f"{current_conversations}/25"

    def get_user_chat_history(self, user_id):
        """Lấy lịch sử chat của user theo thời gian"""
        if user_id not in self.user_memories:
            return "User này chưa có lịch sử chat với bot."

        messages = self.user_memories[user_id]
        if not messages:
            return "User này chưa có tin nhắn nào."

        history = []
        for msg in messages:
            if msg["role"] == "user":
                timestamp = msg.get("timestamp", "N/A")
                content = msg["content"]
                # Rút gọn tin nhắn nếu quá dài
                if len(content) > 100:
                    content = content[:100] + "..."
                history.append(f"🕐 {timestamp}\n👤 User: {content}")

        return "\n\n".join(history)

    def get_users_with_chat_history(self):
        """Lấy danh sách user có lịch sử chat"""
        users_with_chat = []
        for user_id in self.user_memories:
            if self.user_memories[user_id]:  # Có tin nhắn
                message_count = len(self.user_memories[user_id]) // 2
                users_with_chat.append((user_id, message_count))
        return users_with_chat

    def get_current_time(self):
        vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
        now = datetime.now(vn_tz)
        weekdays = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
        weekday = weekdays[now.weekday()]
        return f"{weekday}, {now.strftime('%d/%m/%Y %H:%M:%S')} (GMT+7)"

    def google_search(self, query, num_results=3):
        try:
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': GOOGLE_SEARCH_API_KEY,
                'cx': GOOGLE_CSE_ID,
                'q': query,
                'num': num_results,
                'hl': 'vi'
            }

            response = requests.get(url, params=params, timeout=API_TIMEOUT)

            if response.status_code != 200:
                return f"Lỗi API: {response.status_code}"

            data = response.json()
            items = data.get('items', [])

            if not items:
                return "Không tìm thấy kết quả nào."

            results = []
            for i, item in enumerate(items[:num_results], 1):
                title = item.get('title', 'Không có tiêu đề')
                link = item.get('link', 'Không có link')
                snippet = item.get('snippet', 'Không có mô tả')

                results.append(f"{i}. 📰 {title}\n🔗 {link}\n📝 {snippet}\n")

            return "\n".join(results)
        except Exception as e:
            return f"Lỗi khi tìm kiếm: {str(e)}"

    def call_api(self, prompt, user_id=None):
        # Lấy bộ nhớ của user
        user_memory = ""
        current_time = self.get_current_time()

        if user_id and user_id in self.user_memories:
            memory_list = self.user_memories[user_id][-20:]  # Lấy 20 tin nhắn gần nhất
            for msg in memory_list:
                if msg["role"] == "user":
                    user_memory += f"User đã hỏi: {msg['content']}\n"
                else:
                    user_memory += f"AI đã trả lời: {msg['content']}\n"
            if user_memory:
                user_memory = f"\n=== Cuộc trò chuyện trước đó ===\n{user_memory}=== Tin nhắn hiện tại ===\n"

        try:
            headers = {'Content-Type': 'application/json'}

            # Tạo context với training text và bộ nhớ
            full_context = f"{TRAINING_TEXT}\n\nThời gian hiện tại: {current_time}\n{user_memory}\n"

            # Enhanced prompt
            enhanced_prompt = f"{full_context}\n\nUser hiện tại hỏi: {prompt}\n\nHãy trả lời một cách tự nhiên. Không bắt đầu bằng 'Zyah King👽:' trong phản hồi."

            data = {
                "contents": [
                    {
                        "parts": [
                            {
                                "text": enhanced_prompt
                            }
                        ]
                    }
                ],
                "generationConfig": {
                    "temperature": 0.8,
                    "topK": 50,
                    "topP": 0.9,
                    "maxOutputTokens": 8192,
                    "stopSequences": ["Zyah King👽:", "Zyah King👽: Zyah King👽:"],
                    "responseMimeType": "text/plain"
                }
            }

            # Gọi API Gemini 2.0 Flash
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key={self.gemini_key}"
            response = requests.post(url, json=data, headers=headers, timeout=API_TIMEOUT)

            if response.status_code == 200:
                result = response.json()
                if 'candidates' in result and len(result['candidates']) > 0:
                    content = result['candidates'][0]['content']['parts'][0]['text']
                    return content.strip()
                else:
                    return "Xin lỗi, tôi không thể tạo phản hồi phù hợp cho câu hỏi này."
            else:
                return f"Lỗi API: {response.status_code} - {response.text}"

        except Exception as e:
            return f"Đã xảy ra lỗi: {str(e)}"