"""
通风报信插件 - 帮你传话给好友，支持来回对话
"""
import os
import re
import gc
import json
import asyncio
from datetime import datetime
import zoneinfo
from typing import Any, Dict, Optional, Tuple
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api.message_components import Plain, At, Reply, Image
from astrbot.api import logger, AstrBotConfig

MAX_RECORDS = 500

@register("messenger", "落日七号", "通风报信插件 - 帮你传话给好友，支持来回对话", "1.3.1", "")
class MessengerPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.context = context
        self.config = config or {}
        
        self.enable_llm = self.config.get('enable_llm_recognition', True)
        self.msg_prefix = self.config.get('message_prefix', '📨')
        self.success_prefix = self.config.get('success_prefix', '✅')
        self.error_prefix = self.config.get('error_prefix', '❌')
        
        inbox_settings = self.config.get('inbox_settings', {})
        self.enable_inbox = inbox_settings.get('enable_inbox', False)
        self.owner_qq = str(inbox_settings.get('owner_qq', ''))
        self.inbox_type = inbox_settings.get('inbox_type', 'group')
        self.inbox_id = str(inbox_settings.get('inbox_id', ''))
        
        broadcast_settings = self.config.get('broadcast_settings', {})
        blacklist_str = broadcast_settings.get('blacklist', '')
        self.broadcast_blacklist = set(qq.strip() for qq in blacklist_str.split(',') if qq.strip())
        self.broadcast_delay = broadcast_settings.get('delay_seconds', 1)
        
        # 管理员列表
        admin_str = self.config.get('admin_qq_list', '')
        self.admin_qq_list = set(qq.strip() for qq in admin_str.split(',') if qq.strip())

        # 问话增强配置（接入 context_aware / llmperception）
        enhance_cfg = self.config.get('initiate_chat_enhancement', {})
        self.enable_context_aware_for_chat = bool(enhance_cfg.get('enable_context_aware', True))
        self.context_aware_history_count = max(1, min(50, int(enhance_cfg.get('context_history_count', 10) or 10)))
        self.enable_time_perception_for_chat = bool(enhance_cfg.get('enable_time_perception', True))
        self.perception_timezone = str(enhance_cfg.get('perception_timezone', 'Asia/Shanghai'))
        # 可选：问话时指定系统人格ID；留空则沿用当前会话人格与自定义规则
        self.initiate_chat_persona_id = str(enhance_cfg.get('persona_id', '') or '').strip()

        # 消息记录（实例属性，热重载时自动清理）
        self.message_records: Dict[str, dict] = {}
        self.user_last_received: Dict[str, dict] = {}
        
        # 持久化路径 & 恢复
        try:
            from astrbot.api.star import StarTools
            self._data_dir = StarTools.get_data_dir("messenger")
        except Exception:
            self._data_dir = ""
        self._load_records()
    
    # ==================== 持久化 ====================
    
    def _records_path(self) -> str:
        return os.path.join(self._data_dir, "records.json") if self._data_dir else ""
    
    def _load_records(self):
        path = self._records_path()
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.message_records = data.get('message_records', {})
            self.user_last_received = data.get('user_last_received', {})
            logger.info(f"[Messenger] 已恢复 {len(self.message_records)} 条消息记录")
        except Exception as e:
            logger.error(f"[Messenger] 加载记录失败: {e}")
    
    def _save_records(self):
        path = self._records_path()
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({
                    'message_records': self.message_records,
                    'user_last_received': self.user_last_received
                }, f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"[Messenger] 保存记录失败: {e}")
    
    def _trim_records(self):
        if len(self.message_records) > MAX_RECORDS:
            keys = list(self.message_records.keys())
            for k in keys[:len(keys) - MAX_RECORDS]:
                del self.message_records[k]
    
    # ==================== 帮助命令 ====================
    
    @filter.command("传话帮助", alias={'messenger_help', '传话help'})
    async def show_help(self, event: AstrMessageEvent):
        '''显示传话插件的使用帮助'''
        help_text = """📨 **通风报信插件帮助**

**【命令一览】**
• `传话帮助` - 显示此帮助信息
• `传话 @某人 消息内容` - 传话给好友
• `转发 @某人 消息内容` - 同上（别名）
• `转告 @某人 消息内容` - 同上（别名）
• `传话 QQ号 消息内容` - 用QQ号传话
• `通告群聊 群号 消息内容` - 向群发通告（管理员）
• `问话 QQ号/群号 主题` - 主动发起聊天（管理员）
• `群发 消息内容` - 一键群发（管理员）

**【回复传话】**
引用传话消息，直接发送回复内容即可

**【问话功能】**
管理员发送 `问话 QQ号/群号 主题（选填）`
Bot 会根据该对话的上下文，用 LLM 生成一条自然的聊天消息并发送
如果填写了主题，会围绕该主题展开

**【注意事项】**
• 传话目标必须是 bot 的好友
• 支持发送图片，图文会一起转发
• 通告群聊、群发和问话仅管理员可用"""
        
        yield event.plain_result(help_text)
    
    # ==================== 工具方法 ====================
    
    def _is_admin(self, sender_id: str) -> bool:
        """检查是否是管理员（未配置管理员列表时，管理员功能不可用）"""
        if not self.admin_qq_list:
            return False
        return str(sender_id) in self.admin_qq_list
    
    async def _check_friend(self, event: AstrMessageEvent, qq: str) -> Tuple[bool, Optional[str]]:
        """检查是否是好友"""
        try:
            if event.get_platform_name() == "aiocqhttp":
                from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
                if isinstance(event, AiocqhttpMessageEvent):
                    friend_list = await event.bot.api.call_action('get_friend_list')
                    for friend in friend_list:
                        if str(friend.get('user_id', '')) == str(qq):
                            return True, friend.get('nickname', str(qq))
            return False, None
        except Exception as e:
            logger.error(f"检查好友列表失败: {e}")
            return False, None
    
    async def _check_group(self, event: AstrMessageEvent, group_id: str) -> Tuple[bool, Optional[str]]:
        """检查 bot 是否在指定群中"""
        try:
            if event.get_platform_name() == "aiocqhttp":
                from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
                if isinstance(event, AiocqhttpMessageEvent):
                    group_list = await event.bot.api.call_action('get_group_list')
                    for group in group_list:
                        if str(group.get('group_id', '')) == str(group_id):
                            return True, group.get('group_name', str(group_id))
            return False, None
        except Exception as e:
            logger.error(f"检查群列表失败: {e}")
            return False, None
    
    async def _get_group_name(self, event: AstrMessageEvent, group_id: str) -> str:
        """获取群名称"""
        try:
            if event.get_platform_name() == "aiocqhttp":
                from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
                if isinstance(event, AiocqhttpMessageEvent):
                    info = await event.bot.api.call_action('get_group_info', group_id=int(group_id))
                    return info.get('group_name', str(group_id))
        except Exception as e:
            logger.error(f"获取群信息失败: {e}")
        return str(group_id)
    
    def _is_inbox_group(self, group_id) -> bool:
        """判断是否是收件箱群聊"""
        return bool(self.enable_inbox and self.inbox_type == 'group' and self.inbox_id and str(group_id) == str(self.inbox_id))
    
    async def _get_real_name(self, event: AstrMessageEvent, sender_id: str, default_name: str) -> str:
        """收件箱场景下获取真实QQ昵称（群昵称可能不是真名）"""
        group_id = event.message_obj.group_id
        if not group_id or not self._is_inbox_group(group_id):
            return default_name
        try:
            if event.get_platform_name() == "aiocqhttp":
                from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
                if isinstance(event, AiocqhttpMessageEvent):
                    info = await event.bot.api.call_action('get_stranger_info', user_id=int(sender_id))
                    return info.get('nickname', default_name)
        except Exception:
            pass
        return default_name
    
    def _format_sender_info(self, sender_name: str, sender_id: str, group_name: str = None) -> str:
        """格式化发送者信息"""
        if group_name:
            return f"「{group_name}」的 {sender_name}({sender_id})"
        return f"{sender_name}({sender_id})"
    
    def _extract_all_content(self, event: AstrMessageEvent, skip_command: bool = False) -> str:
        """
        提取消息的所有内容（包含图片），保持原始顺序
        skip_command: 是否跳过命令头和 @ 部分
        """
        parts = []
        command_skipped = not skip_command
        at_found = False
        bot_id = self._get_bot_id(event)
        
        for comp in event.message_obj.message:
            if isinstance(comp, Reply):
                command_skipped = True  # 引用回复时无命令头需跳过，直接标记
                continue
            elif isinstance(comp, At):
                qq = comp.qq if hasattr(comp, 'qq') else None
                # 跳过 bot 自身的 @（唤醒词），不影响状态
                if qq and str(qq) == bot_id:
                    continue
                at_found = True
                if skip_command:
                    command_skipped = True
                continue
            elif isinstance(comp, Plain):
                text = comp.text if hasattr(comp, 'text') else str(comp)
                
                # 跳过引用消息的文本标记
                if '[引用消息' in text:
                    match = re.search(r'\[引用消息[^\]]*\]\s*(.*)', text, re.DOTALL)
                    if match:
                        text = match.group(1).strip()
                    else:
                        continue
                
                # 跳过系统提示
                if '[系统提示' in text:
                    match = re.search(r'\[系统提示[^\]]*\]\s*(.*)', text, re.DOTALL)
                    if match:
                        text = match.group(1).strip()
                    else:
                        continue
                
                # 跳过命令头
                if skip_command and not command_skipped:
                    cmd_match = re.match(r'^/?(?:传话|转发|转告|群发|broadcast|一键群发|通告群聊|群聊通告)\s*', text, re.IGNORECASE)
                    if cmd_match:
                        text = text[cmd_match.end():]
                        command_skipped = True
                    # 跳过 @ 或 QQ号/群号
                    at_match = re.match(r'(?:\[At:\d+\]|@[^\s]*(?:\(\d+\))?|\d{5,11})\s*', text)
                    if at_match:
                        text = text[at_match.end():]
                    elif at_found:
                        command_skipped = True
                
                if text.strip():
                    parts.append(text.strip())
                    
            elif isinstance(comp, Image):
                if skip_command and not command_skipped and not at_found:
                    continue
                img_url = comp.url if hasattr(comp, 'url') and comp.url else (comp.file if hasattr(comp, 'file') else None)
                if img_url:
                    parts.append(f"[CQ:image,file={img_url}]")
                if skip_command:
                    command_skipped = True
        
        return " ".join(parts) if parts else ""
    
    def _get_bot_id(self, event: AstrMessageEvent) -> Optional[str]:
        """获取 bot 自身的 QQ 号"""
        try:
            self_id = getattr(event.message_obj, 'self_id', None)
            if self_id:
                return str(self_id)
        except Exception:
            pass
        return None
    
    def _extract_target_qq(self, event: AstrMessageEvent, message_str: str) -> Optional[str]:
        """从消息中提取目标 QQ 号（跳过 bot 自身的 @）"""
        bot_id = self._get_bot_id(event)
        for comp in event.message_obj.message:
            if isinstance(comp, At):
                qq = comp.qq if hasattr(comp, 'qq') else None
                if qq and str(qq) != bot_id:
                    return str(qq)
        
        patterns = [
            r'\[At:(\d{5,11})\]',
            r'@[^\(]+\((\d{5,11})\)',
            r'@(\d{5,11})',
            r'(?:传话|转发|转告)\s*(\d{5,11})',
        ]
        for pattern in patterns:
            match = re.search(pattern, message_str)
            if match:
                return match.group(1)
        
        return None
    
    def _extract_target_group(self, message_str: str) -> Optional[str]:
        """从消息中提取目标群号"""
        # 匹配 "通告群聊 群号" 格式
        match = re.search(r'(?:通告群聊|群聊通告)\s*(\d{5,11})', message_str, re.IGNORECASE)
        if match:
            return match.group(1)
        return None
    
    def _extract_reply_target(self, message_str: str) -> Optional[Tuple[str, str]]:
        """从引用消息中提取回复目标（发送者）"""
        if '[引用消息' not in message_str:
            return None
        
        # 方法1：严格匹配
        pattern = rf'\[引用消息\([^:]+:\s*{re.escape(self.msg_prefix)} (?:「[^」]+」的 )?([^\(]+)\((\d+)\) (?:对你说|让我回复你|通告)：'
        match = re.search(pattern, message_str, re.DOTALL)
        if match:
            sender_name = match.group(1).strip()
            sender_qq = match.group(2)
            logger.info(f"[Messenger] 从引用消息提取发送者: {sender_name}({sender_qq})")
            return sender_name, sender_qq
        
        # 方法2：备用匹配
        quote_match = re.search(r'\[引用消息\([^:]+:\s*([^\]]+)\]', message_str, re.DOTALL)
        if quote_match:
            quote_content = quote_match.group(1)
            pattern2 = rf'{re.escape(self.msg_prefix)}\s*(?:「[^」]+」的\s*)?([^\(]+)\((\d+)\)\s*(?:对你说|让我回复你|通告)'
            match2 = re.search(pattern2, quote_content, re.DOTALL)
            if match2:
                sender_name = match2.group(1).strip()
                sender_qq = match2.group(2)
                logger.info(f"[Messenger] 从引用消息提取发送者(备用): {sender_name}({sender_qq})")
                return sender_name, sender_qq
        
        return None
    
    def _has_reply(self, event: AstrMessageEvent) -> Optional[str]:
        """检查是否有引用消息，返回引用消息 ID"""
        for comp in event.message_obj.message:
            if isinstance(comp, Reply):
                return str(comp.id)
        
        message_str = event.message_str
        if '[引用消息' in message_str:
            return "from_text"
        
        return None
    
    def _is_tell_command(self, message: str) -> bool:
        """检查是否是传话命令"""
        return bool(re.search(r'(?:^|[\s/])(?:传话|转发|转告)(?:\s|@|\d|$)', message, re.IGNORECASE))
    
    def _is_broadcast_command(self, event_or_str) -> bool:
        """检查是否是群发命令，支持传入 event 或 str"""
        if isinstance(event_or_str, str):
            message = event_or_str
        else:
            # 从消息组件中提取纯文本，非文本组件用空格占位
            message = ""
            for comp in event_or_str.message_obj.message:
                if isinstance(comp, Plain):
                    text = comp.text if hasattr(comp, 'text') else str(comp)
                    message += (text or "")
                else:
                    message += " "
        return bool(re.search(r'(?:^|[\s/])(?:群发|broadcast|一键群发)', message, re.IGNORECASE))
    
    def _is_group_announce_command(self, message: str) -> bool:
        """检查是否是通告群聊命令"""
        return bool(re.search(r'(?:^|[\s/])(?:通告群聊|群聊通告)(?:\s|\d|$)', message, re.IGNORECASE))
    
    def _is_initiate_chat_command(self, message: str) -> bool:
        """检查是否是问话命令"""
        return bool(re.search(r'(?:^|[\s/])(?:问话|搭话|主动聊天)(?:\s|@|\d|$)', message, re.IGNORECASE))
    
    async def _send_private_message(self, event: AstrMessageEvent, qq: str, message: str, reply_to_msg_id: str = None) -> Optional[str]:
        """发送私聊消息"""
        try:
            if event.get_platform_name() == "aiocqhttp":
                from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
                if isinstance(event, AiocqhttpMessageEvent):
                    if reply_to_msg_id:
                        message = f"[CQ:reply,id={reply_to_msg_id}]{message}"
                    result = await event.bot.api.call_action('send_private_msg', user_id=int(qq), message=message)
                    return str(result.get('message_id', ''))
        except Exception as e:
            logger.error(f"发送私聊消息失败: {e}")
        return None
    
    async def _send_group_message(self, event: AstrMessageEvent, group_id: str, message: str, reply_to_msg_id: str = None) -> Optional[str]:
        """发送群聊消息"""
        try:
            if event.get_platform_name() == "aiocqhttp":
                from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
                if isinstance(event, AiocqhttpMessageEvent):
                    if reply_to_msg_id:
                        message = f"[CQ:reply,id={reply_to_msg_id}]{message}"
                    result = await event.bot.api.call_action('send_group_msg', group_id=int(group_id), message=message)
                    return str(result.get('message_id', ''))
        except Exception as e:
            logger.error(f"发送群聊消息失败: {e}")
        return None
    
    async def _send_to_user(self, event: AstrMessageEvent, target_qq: str, message: str, reply_to_msg_id: str = None) -> Optional[str]:
        """发送消息给用户（支持收件箱转发）"""
        if self.enable_inbox and self.inbox_id and self.owner_qq and str(target_qq) == str(self.owner_qq):
            if self.inbox_type == 'group':
                return await self._send_group_message(event, self.inbox_id, message, reply_to_msg_id)
            return await self._send_private_message(event, self.inbox_id, message, reply_to_msg_id)
        return await self._send_private_message(event, target_qq, message, reply_to_msg_id)
    
    async def _llm_parse_tell_intent(self, message: str) -> Optional[Tuple[str, str]]:
        """使用 LLM 智能识别传话意图"""
        if not self.enable_llm:
            return None
        
        try:
            provider = self.context.get_using_provider()
            if not provider:
                return None
            
            prompt = f"""分析以下消息，判断用户是否想要传话给某人。

消息内容："{message}"

请以 JSON 格式返回分析结果：
{{
    "is_tell": true 或 false,
    "target_qq": "目标QQ号（如果能识别到数字）或 null",
    "content": "要传达的消息内容 或 null",
    "confidence": 0.0-1.0 的置信度
}}

判断规则：
1. 如果消息包含"告诉"、"转告"、"传话"、"跟...说"、"帮我说"等意图，且有明确的目标（QQ号或@某人）和内容，is_tell 为 true
2. 如果不是传话相关的消息，is_tell 为 false
3. 只有当 confidence >= 0.7 时才认为识别成功

只返回 JSON，不要其他内容。"""

            response = await provider.text_chat(
                prompt=prompt,
                session_id=None,
                contexts=[],
                image_urls=[],
                system_prompt="你是一个意图识别助手，只返回 JSON 格式的结果。"
            )
            
            if response and response.completion_text:
                text = response.completion_text.strip()
                if text.startswith("```"):
                    text = re.sub(r'^```(?:json)?\s*', '', text)
                    text = re.sub(r'\s*```$', '', text)
                
                result = json.loads(text)
                logger.info(f"[Messenger] LLM 意图识别结果: {result}")
                
                if result.get('confidence', 0) >= 0.7 and result.get('is_tell') and result.get('target_qq'):
                    return result['target_qq'], result.get('content', '')
                    
        except json.JSONDecodeError as e:
            logger.debug(f"[Messenger] LLM 返回的 JSON 解析失败: {e}")
        except Exception as e:
            logger.error(f"[Messenger] LLM 意图识别失败: {e}")
        
        return None
    
    # ==================== 统一消息处理器 ====================
    
    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        """统一消息处理器，按优先级处理：引用回复 > 通告群聊 > 传话命令 > 群发命令"""
        message_str = event.message_str
        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name()
        
        logger.debug(f"[Messenger] on_message: message_str='{(message_str or '')[:80]}', "
                     f"components={[type(c).__name__ for c in event.message_obj.message]}")
        
        # ========== 优先级1：引用传话消息 = 回复 ==========
        reply_msg_id = self._has_reply(event)
        if reply_msg_id:
            target_qq = None
            target_name = None
            is_group_reply = False
            
            if reply_msg_id in self.message_records:
                record = self.message_records[reply_msg_id]
                is_group_reply = record.get('is_group_announce', False)
                is_group_broadcast = record.get('is_group', False)  # 群发消息标记
                
                target_qq = record['from_user'] if str(sender_id) == str(record['to_user']) else record['to_user']
                target_name = record['from_name'] if str(sender_id) == str(record['to_user']) else record['to_name']
                
                # 群聊通告或群发的回复
                if is_group_reply or is_group_broadcast:
                    # 群成员回复 -> 转发给发件人
                    # 发件人自己回复 -> 忽略（群号不能当QQ号私聊）
                    if str(sender_id) != str(record['from_user']):
                        target_qq = record['from_user']
                        target_name = record['from_name']
                    else:
                        return  # 发件人自己回复群广播，不处理
            else:
                target_info = self._extract_reply_target(message_str)
                if target_info:
                    target_name, target_qq = target_info
            
            if target_qq:
                content = self._extract_all_content(event, skip_command=True)
                if not content:
                    return
                
                group_id = event.message_obj.group_id
                group_name = None if not group_id or self._is_inbox_group(group_id) else await self._get_group_name(event, str(group_id))
                sender_name = await self._get_real_name(event, sender_id, sender_name)
                
                logger.info(f"[Messenger] 回复: {sender_name} -> {target_name}: {content[:50]}...")
                
                sender_info = self._format_sender_info(sender_name, sender_id, group_name)
                reply_msg = f"{self.msg_prefix} {sender_info} 让我回复你：\n{content}"
                
                # 回复始终发送到私聊（通过 _send_to_user 支持收件箱）
                new_msg_id = await self._send_to_user(event, target_qq, reply_msg)
                if new_msg_id:
                    self.message_records[new_msg_id] = {
                        "from_user": sender_id,
                        "to_user": target_qq,
                        "from_name": sender_name,
                        "to_name": target_name,
                        "original_msg_id": new_msg_id
                    }
                    self._trim_records()
                    self.user_last_received[target_qq] = {
                        "from_user": sender_id,
                        "from_name": sender_name,
                        "msg_id": new_msg_id
                    }
                    self._save_records()
                    yield event.plain_result(f"{self.success_prefix} 已将你的回复转达给 {target_name}！")
                else:
                    yield event.plain_result(f"{self.error_prefix} 消息发送失败。")
                event.stop_event()
                return
        
        # ========== 优先级2：问话命令（仅管理员） ==========
        if self._is_initiate_chat_command(message_str):
            if not self._is_admin(sender_id):
                yield event.plain_result(f"{self.error_prefix} 问话功能仅管理员可用。请在插件配置中添加你的QQ号到管理员列表。")
                event.stop_event()
                return
            async for result in self._do_initiate_chat(event):
                yield result
            event.stop_event()
            return
        
        # ========== 优先级3：通告群聊命令（仅管理员） ==========
        if self._is_group_announce_command(message_str):
            if not self._is_admin(sender_id):
                yield event.plain_result(f"{self.error_prefix} 通告群聊功能仅管理员可用。请在插件配置中添加你的QQ号到管理员列表。")
                event.stop_event()
                return
            async for result in self._do_group_announce(event):
                yield result
            event.stop_event()
            return
        
        # ========== 优先级4：群发命令（仅管理员） ==========
        if self._is_broadcast_command(event):
            if not self._is_admin(sender_id):
                yield event.plain_result(f"{self.error_prefix} 群发功能仅管理员可用。请在插件配置中添加你的QQ号到管理员列表。")
                event.stop_event()
                return
            async for result in self._do_broadcast(event):
                yield result
            event.stop_event()
            return
        
        # ========== 优先级5：传话命令 ==========
        if self._is_tell_command(message_str):
            async for result in self._do_tell(event):
                yield result
            event.stop_event()
            return
    
    # ==================== 问话（主动聊天） ====================

    def _extract_text_from_message_content(self, content: Any) -> str:
        """从消息 content 中提取纯文本（兼容 str/list[dict]/对象）"""
        if content is None:
            return ""
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts = []
            for part in content:
                if isinstance(part, dict):
                    text = part.get("text", "")
                    if text:
                        parts.append(str(text))
                elif hasattr(part, "text"):
                    text = getattr(part, "text", "")
                    if text:
                        parts.append(str(text))
                elif isinstance(part, str):
                    parts.append(part)
            return " ".join(p.strip() for p in parts if str(p).strip()).strip()
        return str(content).strip()

    async def _get_current_session_system_prompt(self, event: AstrMessageEvent) -> Optional[str]:
        """
        获取当前会话的 system_prompt，按优先级尝试：
        1. conversation_manager 当前对话的 persona_id
        2. persona_manager.get_default_persona_v3(umo) - 会话绑定人格
        3. persona_manager.get_default_persona_v3(None) - 全局默认人格
        """
        current_umo = getattr(event, "unified_msg_origin", None)
        persona_mgr = getattr(self.context, "persona_manager", None)
        conv_mgr = getattr(self.context, "conversation_manager", None)

        # 方法1：从 conversation_manager 获取当前对话绑定的 persona_id
        if conv_mgr and current_umo:
            try:
                curr_cid = await conv_mgr.get_curr_conversation_id(current_umo)
                if curr_cid:
                    conversation = await conv_mgr.get_conversation(current_umo, curr_cid)
                    if conversation:
                        persona_id = getattr(conversation, "persona_id", None)
                        if persona_id and persona_mgr:
                            try:
                                persona = persona_mgr.get_persona(persona_id)
                                if asyncio.iscoroutine(persona):
                                    persona = await persona
                                if persona:
                                    sp = getattr(persona, "prompt", None) or getattr(persona, "system_prompt", None)
                                    if sp and str(sp).strip():
                                        logger.info(f"[Messenger] 问话人格：通过当前对话 persona_id={persona_id} 获取成功")
                                        return str(sp).strip()
                            except Exception as e:
                                logger.debug(f"[Messenger] 通过 persona_id 获取人格失败: {e}")
            except Exception as e:
                logger.debug(f"[Messenger] 从 conversation_manager 获取 persona_id 失败: {e}")

        if not persona_mgr:
            logger.debug("[Messenger] persona_manager 不存在")
            return None

        # 方法2：尝试当前会话绑定的人格
        try:
            persona = persona_mgr.get_default_persona_v3(current_umo)
            if asyncio.iscoroutine(persona):
                persona = await persona
            if persona:
                sp = getattr(persona, "prompt", None) or getattr(persona, "system_prompt", None)
                if sp and str(sp).strip():
                    logger.info("[Messenger] 问话人格：通过 get_default_persona_v3(umo) 获取成功")
                    return str(sp).strip()
        except Exception as e:
            logger.debug(f"[Messenger] get_default_persona_v3(umo) 失败: {e}")

        # 方法3：尝试全局默认人格（传入 None）
        try:
            persona = persona_mgr.get_default_persona_v3(None)
            if asyncio.iscoroutine(persona):
                persona = await persona
            if persona:
                sp = getattr(persona, "prompt", None) or getattr(persona, "system_prompt", None)
                if sp and str(sp).strip():
                    logger.info("[Messenger] 问话人格：通过全局默认人格获取成功")
                    return str(sp).strip()
        except Exception as e:
            logger.debug(f"[Messenger] get_default_persona_v3(None) 失败: {e}")

        return None

    async def _resolve_initiate_chat_system_prompt(self, event: AstrMessageEvent, target_umo: str = None) -> Optional[str]:
        """
        解析问话使用的 system prompt：
        - 未配置 persona_id：通过目标会话（target_umo）的人格
        - 配置 persona_id：通过 persona_manager.get_persona(persona_id) 获取指定人格
        """
        persona_id = (self.initiate_chat_persona_id or "").strip()
        persona_mgr = getattr(self.context, "persona_manager", None)
        conv_mgr = getattr(self.context, "conversation_manager", None)

        if not persona_id:
            # 未指定 persona_id，使用目标会话的人格
            if target_umo:
                target_prompt = await self._get_session_system_prompt_by_umo(target_umo)
                if target_prompt:
                    return target_prompt
            # 回退：使用当前会话人格
            session_prompt = await self._get_current_session_system_prompt(event)
            if session_prompt:
                return session_prompt
            logger.info("[Messenger] 问话人格：目标会话和当前会话均无绑定人格，使用框架默认链路")
            return None

        # 指定了 persona_id，尝试通过 persona_manager.get_persona 获取
        if persona_mgr and callable(getattr(persona_mgr, "get_persona", None)):
            try:
                persona = persona_mgr.get_persona(persona_id)
                if asyncio.iscoroutine(persona):
                    persona = await persona
                if persona:
                    sp = getattr(persona, "system_prompt", None) or getattr(persona, "prompt", None)
                    if sp and str(sp).strip():
                        logger.info(f"[Messenger] 问话人格：已使用指定人格 {persona_id} (persona_manager.get_persona)")
                        return str(sp).strip()
            except Exception as e:
                logger.debug(f"[Messenger] persona_manager.get_persona({persona_id}) 失败: {e}")

        # 兜底：回退目标会话人格 -> 当前会话人格
        logger.warning(f"[Messenger] 未找到人格ID={persona_id} 对应设定，尝试回退目标/当前会话人格")
        if target_umo:
            target_prompt = await self._get_session_system_prompt_by_umo(target_umo)
            if target_prompt:
                return target_prompt
        session_prompt = await self._get_current_session_system_prompt(event)
        if session_prompt:
            return session_prompt
        logger.info("[Messenger] 问话人格：回退为框架默认链路")
        return None

    async def _get_session_system_prompt_by_umo(self, umo: str) -> Optional[str]:
        """
        根据指定 UMO 获取该会话的 system_prompt
        """
        persona_mgr = getattr(self.context, "persona_manager", None)
        conv_mgr = getattr(self.context, "conversation_manager", None)

        # 方法1：从 conversation_manager 获取该会话的 persona_id
        if conv_mgr and umo:
            try:
                curr_cid = await conv_mgr.get_curr_conversation_id(umo)
                if curr_cid:
                    conversation = await conv_mgr.get_conversation(umo, curr_cid)
                    if conversation:
                        persona_id = getattr(conversation, "persona_id", None)
                        if persona_id and persona_mgr:
                            try:
                                persona = persona_mgr.get_persona(persona_id)
                                if asyncio.iscoroutine(persona):
                                    persona = await persona
                                if persona:
                                    sp = getattr(persona, "prompt", None) or getattr(persona, "system_prompt", None)
                                    if sp and str(sp).strip():
                                        logger.info(f"[Messenger] 问话人格：通过目标会话 persona_id={persona_id} 获取成功")
                                        return str(sp).strip()
                            except Exception as e:
                                logger.debug(f"[Messenger] 通过目标会话 persona_id 获取人格失败: {e}")
            except Exception as e:
                logger.debug(f"[Messenger] 从目标会话 conversation_manager 获取 persona_id 失败: {e}")

        if not persona_mgr:
            return None

        # 方法2：尝试该会话绑定的人格
        try:
            persona = persona_mgr.get_default_persona_v3(umo)
            if asyncio.iscoroutine(persona):
                persona = await persona
            if persona:
                sp = getattr(persona, "prompt", None) or getattr(persona, "system_prompt", None)
                if sp and str(sp).strip():
                    logger.info(f"[Messenger] 问话人格：通过目标会话 get_default_persona_v3 获取成功")
                    return str(sp).strip()
        except Exception as e:
            logger.debug(f"[Messenger] 目标会话 get_default_persona_v3 失败: {e}")

        return None

    async def _do_initiate_chat(self, event: AstrMessageEvent):
        """执行问话 - 主动在目标对话中基于上下文发起聊天"""
        message_str = event.message_str
        
        # 提取目标 ID（QQ号或群号）
        target_id = self._extract_target_qq(event, message_str)
        
        if not target_id:
            match = re.search(r'(?:问话|搭话|主动聊天)\s*(\d{5,11})', message_str)
            if match:
                target_id = match.group(1)
        
        if not target_id:
            yield event.plain_result(
                f"{self.error_prefix} 请指定目标QQ号或群号。\n"
                f"用法: 问话 QQ号/群号 主题（选填）\n"
                f"示例: 问话 123456 最近天气怎么样"
            )
            return
        
        # 提取主题（选填）
        topic = self._extract_chat_topic(event, target_id)
        
        # 判断目标是群还是好友
        is_group = False
        target_name = target_id
        
        in_group, group_name = await self._check_group(event, target_id)
        if in_group:
            is_group = True
            target_name = group_name or target_id
        else:
            is_friend, friend_name = await self._check_friend(event, target_id)
            if is_friend:
                target_name = friend_name or target_id
            else:
                yield event.plain_result(f"{self.error_prefix} {target_id} 既不在我的好友列表中，也不在我加入的群列表中。")
                return
        
        # 获取增强上下文（优先 context_aware，回退会话管理器）
        context_text = await self._get_augmented_context(event, target_id, is_group)
        # 获取时间/节假日感知（优先 llmperception，失败时降级）
        time_perception_text = await self._get_time_perception_text(event)
        
        # 调用 LLM 生成消息
        try:
            provider_id = None
            try:
                provider_id = await self.context.get_current_chat_provider_id(umo=event.unified_msg_origin)
            except Exception:
                pass
            
            if not provider_id:
                # 回退：尝试使用旧版 API
                try:
                    provider = self.context.get_using_provider()
                    if provider:
                        provider_id = "__fallback__"
                except Exception:
                    pass
            
            if not provider_id:
                yield event.plain_result(f"{self.error_prefix} 未找到可用的 LLM 提供商，请先配置大模型。")
                return
            
            target_desc_for_prompt = f"群聊「{target_name}」" if is_group else f"好友「{target_name}」"
            topic_instruction = f"围绕「{topic}」这个话题，" if topic else ""
            context_section = f"\n\n以下是最近的对话历史（供参考，可以延续话题或自然地换一个话题）：\n{context_text}" if context_text else ""
            time_perception_section = (
                f"\n\n以下是当前时间与环境信息（请结合这些信息调整语气与话题）：\n{time_perception_text}"
                if time_perception_text else ""
            )
            
            prompt = (
                f"你需要{topic_instruction}主动向{target_desc_for_prompt}发起一条自然的聊天消息。\n\n"
                f"要求：\n"
                f"1. 消息要自然、随意，像朋友之间的日常闲聊\n"
                f"2. 保持简短，一两句话即可，不要太长\n"
                f"3. 只需要直接输出要发送的消息内容，不要加引号、前缀或任何解释\n"
                f"4. 可以适当使用 emoji\n"
                f"5. 语气轻松活泼，有个性"
                f"{context_section}"
                f"{time_perception_section}"
            )
            
            # 构造目标会话的 UMO，用于获取目标会话的人格
            target_umos = self._build_possible_umos(event, target_id, is_group)
            target_umo = target_umos[0] if target_umos else None
            
            # 获取人格：优先目标会话人格，回退当前会话人格
            system_prompt = await self._resolve_initiate_chat_system_prompt(event, target_umo)
            
            generated_msg = None
            current_umo = getattr(event, "unified_msg_origin", None)

            if not system_prompt and current_umo:
                logger.info(f"[Messenger] 问话人格：尝试继承当前会话人格与规则 (UMO={current_umo})")
            
            if provider_id == "__fallback__":
                # 旧版 API 回退
                provider = self.context.get_using_provider()
                kwargs = {
                    "prompt": prompt,
                    "session_id": current_umo if current_umo else None,
                    "contexts": [],
                    "image_urls": [],
                }
                if system_prompt:
                    kwargs["system_prompt"] = system_prompt
                try:
                    response = await provider.text_chat(**kwargs)
                except TypeError as e:
                    # 兼容极老版本 text_chat 不接受 session_id
                    logger.warning(f"[Messenger] provider.text_chat 参数不兼容，回退无 session_id 调用: {e}")
                    kwargs.pop("session_id", None)
                    response = await provider.text_chat(**kwargs)
                if response and response.completion_text:
                    generated_msg = response.completion_text.strip()
            else:
                kwargs = {
                    "chat_provider_id": provider_id,
                    "prompt": prompt,
                }
                if system_prompt:
                    kwargs["system_prompt"] = system_prompt
                # 关键：把当前会话 UMO 传给框架，继承 /persona 与该会话规则链
                if current_umo:
                    kwargs["umo"] = current_umo

                try:
                    llm_resp = await self.context.llm_generate(**kwargs)
                except TypeError as e:
                    # 兼容不支持 umo 参数的版本，降级但保留日志
                    if "umo" in kwargs:
                        logger.warning(f"[Messenger] llm_generate 不支持 umo 参数，已降级调用: {e}")
                        kwargs.pop("umo", None)
                        llm_resp = await self.context.llm_generate(**kwargs)
                    else:
                        raise
                if llm_resp and llm_resp.completion_text:
                    generated_msg = llm_resp.completion_text.strip()
            
            if not generated_msg:
                yield event.plain_result(f"{self.error_prefix} LLM 生成消息失败，请稍后重试。")
                return
            
            # 清理 LLM 输出中可能的引号包裹
            generated_msg = re.sub(r'^["""\'\'「」『』]+|["""\'\'「」『』]+$', '', generated_msg).strip()
            
            if not generated_msg:
                yield event.plain_result(f"{self.error_prefix} LLM 生成了空消息，请重试。")
                return
            
            # 发送消息到目标
            if is_group:
                msg_id = await self._send_group_message(event, target_id, generated_msg)
            else:
                msg_id = await self._send_private_message(event, target_id, generated_msg)
            
            if msg_id:
                topic_info = f"\n📌 主题：{topic}" if topic else ""
                target_desc = f"群「{target_name}」({target_id})" if is_group else f"{target_name}({target_id})"
                yield event.plain_result(
                    f"{self.success_prefix} 已向 {target_desc} 发送问话{topic_info}\n"
                    f"💬 内容：{generated_msg}"
                )
            else:
                yield event.plain_result(f"{self.error_prefix} 消息发送失败，请检查 bot 是否能向该目标发消息。")
        
        except Exception as e:
            logger.error(f"[Messenger] 问话功能出错: {e}")
            yield event.plain_result(f"{self.error_prefix} 问话失败: {str(e)}")
    
    def _extract_chat_topic(self, event: AstrMessageEvent, target_id: str) -> str:
        """从问话命令中提取主题"""
        text = event.message_str
        # 移除命令前缀
        text = re.sub(r'^/?(?:问话|搭话|主动聊天)\s*', '', text)
        # 移除 @ 提及
        text = re.sub(r'@\S+\s*', '', text)
        # 移除 [At:xxx] 格式
        text = re.sub(r'\[At:\d+\]\s*', '', text)
        # 移除目标号码
        if target_id:
            text = text.replace(target_id, '', 1)
        return text.strip()

    def _build_possible_umos(self, event: AstrMessageEvent, target_id: str, is_group: bool) -> list[str]:
        """构造目标会话的可能 UMO，参考当前会话 UMO 格式"""
        # 从当前会话 UMO 提取前缀（如 default:GroupMessage:xxx -> default）
        current_umo = getattr(event, "unified_msg_origin", "") or ""
        prefix = "default"
        if current_umo and ":" in current_umo:
            prefix = current_umo.split(":")[0]
        
        platform_name = event.get_platform_name()
        
        if is_group:
            return [
                f"{prefix}:GroupMessage:{target_id}",
                f"{platform_name}:GroupMessage:{target_id}",
                f"{prefix}:group:{target_id}",
                f"{platform_name}:group:{target_id}",
            ]
        return [
            f"{prefix}:FriendMessage:{target_id}",
            f"{platform_name}:FriendMessage:{target_id}",
            f"{prefix}:PrivateMessage:{target_id}",
            f"{platform_name}:PrivateMessage:{target_id}",
            f"{prefix}:private:{target_id}",
            f"{platform_name}:private:{target_id}",
        ]

    def _unwrap_plugin_instance(self, obj):
        """从插件管理包装对象中尽量解包出真实插件实例"""
        if obj is None:
            return None

        if isinstance(obj, (str, int, float, bool)):
            return None

        # 已经是插件实例
        if callable(getattr(obj, "terminate", None)):
            return obj

        # 常见包装字段
        for attr in ("instance", "plugin", "star", "obj", "_obj", "target", "value"):
            inner = getattr(obj, attr, None)
            if inner and inner is not obj and callable(getattr(inner, "terminate", None)):
                return inner

        return None

    def _iter_plugins_from_context(self):
        """尽力枚举 context 中已加载插件实例（兼容不同 AstrBot 版本）"""
        seen = set()
        visited_holders = set()
        queue = [self.context]

        # 最大遍历步数，防止极端对象图导致循环
        max_steps = 200
        steps = 0

        while queue and steps < max_steps:
            holder = queue.pop(0)
            steps += 1
            if holder is None:
                continue

            hid = id(holder)
            if hid in visited_holders:
                continue
            visited_holders.add(hid)

            # 先尝试直接解包为插件实例
            direct_instance = self._unwrap_plugin_instance(holder)
            if direct_instance and direct_instance is not self:
                pid = id(direct_instance)
                if pid not in seen:
                    seen.add(pid)
                    yield direct_instance

            values = []

            if isinstance(holder, dict):
                values.extend(holder.values())
            elif isinstance(holder, (list, tuple, set)):
                values.extend(holder)
            else:
                # 兼容常见 manager / registry / container 字段
                for attr in (
                    "plugins", "_plugins", "loaded_plugins", "_loaded_plugins",
                    "stars", "_stars", "loaded_stars", "_loaded_stars",
                    "plugin_manager", "_plugin_manager",
                    "star_manager", "_star_manager",
                    "plugin_registry", "_plugin_registry",
                    "star_registry", "_star_registry",
                    "plugin_map", "_plugin_map",
                    "plugin_dict", "_plugin_dict",
                    "all_plugins", "_all_plugins",
                ):
                    nested = getattr(holder, attr, None)
                    if nested is not None:
                        values.append(nested)

            for item in values:
                if item is None:
                    continue

                # 对 tuple/list 包装做一层展开
                if isinstance(item, (list, tuple, set)):
                    for sub in item:
                        if sub is not None:
                            queue.append(sub)
                    continue

                if isinstance(item, dict):
                    queue.append(item)
                    continue

                # item 本身可能就是插件实例或包装对象
                instance = self._unwrap_plugin_instance(item)
                if instance and instance is not self:
                    pid = id(instance)
                    if pid not in seen:
                        seen.add(pid)
                        yield instance
                    continue

                # 否则继续广度遍历
                queue.append(item)

    def _find_plugin_from_gc(self, required_methods: tuple[str, ...], module_hint: str = ""):
        """兜底：从 GC 对象池扫描插件实例（用于兼容某些版本无法从 context 拿到实例）"""
        try:
            for obj in gc.get_objects():
                try:
                    if obj is self:
                        continue
                    if module_hint:
                        mod = getattr(obj.__class__, "__module__", "") or ""
                        if module_hint not in mod:
                            continue
                    if all(callable(getattr(obj, m, None)) for m in required_methods):
                        return obj
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"[Messenger] GC 扫描插件实例失败: {e}")
        return None

    def _find_context_aware_plugin(self):
        for plugin in self._iter_plugins_from_context():
            if callable(getattr(plugin, "get_formatted_context", None)):
                logger.debug(f"[Messenger] 命中 context_aware 插件实例: {plugin.__class__.__name__}")
                return plugin

        plugin = self._find_plugin_from_gc(
            required_methods=("get_formatted_context", "get_recent_messages"),
            module_hint="astrbot_plugin_context_aware",
        )
        if plugin:
            logger.info(f"[Messenger] 通过 GC 兜底命中 context_aware: {plugin.__class__.__name__}")
            return plugin

        logger.debug("[Messenger] 未发现 context_aware 插件实例")
        return None

    def _find_llmperception_plugin(self):
        for plugin in self._iter_plugins_from_context():
            if callable(getattr(plugin, "_get_holiday_info", None)):
                logger.debug(f"[Messenger] 命中 llmperception 插件实例: {plugin.__class__.__name__}")
                return plugin

        plugin = self._find_plugin_from_gc(
            required_methods=("_get_holiday_info", "_get_platform_info"),
            module_hint="astrbot_plugin_llmperception",
        )
        if plugin:
            logger.info(f"[Messenger] 通过 GC 兜底命中 llmperception: {plugin.__class__.__name__}")
            return plugin

        logger.debug("[Messenger] 未发现 llmperception 插件实例")
        return None

    async def _get_context_aware_context(self, event: AstrMessageEvent, target_id: str, is_group: bool) -> str:
        """通过 context_aware 插件获取上下文"""
        if not self.enable_context_aware_for_chat:
            return ""

        plugin = self._find_context_aware_plugin()
        if not plugin:
            logger.info("[Messenger] context_aware 未接入：未找到插件实例，回退内置上下文")
            return ""

        getter = getattr(plugin, "get_formatted_context", None)
        if not callable(getter):
            return ""

        for umo in self._build_possible_umos(event, target_id, is_group):
            try:
                context_text = getter(umo, self.context_aware_history_count)
                if context_text:
                    logger.info(f"[Messenger] 使用 context_aware 上下文成功: {umo}")
                    return str(context_text)
            except Exception as e:
                logger.debug(f"[Messenger] 调用 context_aware 失败 ({umo}): {e}")

        return ""

    async def _get_time_perception_text(self, event: AstrMessageEvent) -> str:
        """获取时间/节假日感知信息（优先接入 llmperception）"""
        if not self.enable_time_perception_for_chat:
            return ""

        plugin = self._find_llmperception_plugin()
        if plugin:
            try:
                timezone_obj = getattr(plugin, "timezone", None)
                if timezone_obj is None:
                    timezone_obj = zoneinfo.ZoneInfo(self.perception_timezone)
                now = datetime.now(timezone_obj)

                parts = [f"发送时间: {now.strftime('%Y-%m-%d %H:%M:%S')}"]

                for method_name in ("_get_holiday_info", "_get_lunar_info", "_get_solar_term_info", "_get_almanac_info"):
                    method = getattr(plugin, method_name, None)
                    if callable(method):
                        value = method(now)
                        if value:
                            parts.append(str(value))

                platform_method = getattr(plugin, "_get_platform_info", None)
                if callable(platform_method):
                    platform_value = platform_method(event)
                    if asyncio.iscoroutine(platform_value):
                        platform_value = await platform_value
                    if platform_value:
                        parts.append(str(platform_value))

                if len(parts) > 1:
                    logger.info("[Messenger] 使用 llmperception 时间感知成功")
                return " | ".join(parts)
            except Exception as e:
                logger.info(f"[Messenger] llmperception 调用失败，已降级基础时间感知: {e}")

        # 降级策略：仅基础时间信息
        logger.info("[Messenger] llmperception 未接入或未命中，使用基础时间感知")
        try:
            timezone_obj = zoneinfo.ZoneInfo(self.perception_timezone)
        except Exception:
            timezone_obj = zoneinfo.ZoneInfo("Asia/Shanghai")
        now = datetime.now(timezone_obj)

        weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        weekday = weekday_names[now.weekday()]
        period = (
            "上午" if 5 <= now.hour < 12 else
            "中午" if 12 <= now.hour < 14 else
            "下午" if 14 <= now.hour < 18 else
            "晚上" if 18 <= now.hour < 22 else
            "深夜"
        )
        work_state = "周末" if now.weekday() >= 5 else "工作日"
        return f"发送时间: {now.strftime('%Y-%m-%d %H:%M:%S')} | {weekday}, {work_state}, {period}"

    async def _get_augmented_context(self, event: AstrMessageEvent, target_id: str, is_group: bool) -> str:
        """组合 context_aware 与内置会话上下文"""
        context_aware_text = await self._get_context_aware_context(event, target_id, is_group)
        fallback_text = await self._get_conversation_context(event, target_id, is_group)

        if context_aware_text and fallback_text:
            if context_aware_text.strip() == fallback_text.strip():
                return context_aware_text
            return f"[context_aware]\n{context_aware_text}\n\n[conversation_manager]\n{fallback_text}"

        return context_aware_text or fallback_text
    
    async def _get_conversation_context(self, event: AstrMessageEvent, target_id: str, is_group: bool) -> str:
        """获取目标对话的上下文历史"""
        try:
            conv_mgr = self.context.conversation_manager
            if not conv_mgr:
                return ""
            
            platform_name = event.get_platform_name()
            
            # 尝试多种可能的 UMO 格式
            possible_umos = []
            if is_group:
                possible_umos.extend([
                    f"{platform_name}:GroupMessage:{target_id}",
                    f"{platform_name}:group:{target_id}",
                ])
            else:
                possible_umos.extend([
                    f"{platform_name}:FriendMessage:{target_id}",
                    f"{platform_name}:PrivateMessage:{target_id}",
                    f"{platform_name}:private:{target_id}",
                ])
            
            for umo in possible_umos:
                try:
                    curr_cid = await conv_mgr.get_curr_conversation_id(umo)
                    if not curr_cid:
                        continue
                    
                    conversation = await conv_mgr.get_conversation(umo, curr_cid)
                    if not conversation:
                        continue
                    
                    # 尝试从 conversation 中提取历史记录
                    history = getattr(conversation, 'history', None)
                    if not history:
                        history = getattr(conversation, 'messages', None)
                    if not history:
                        continue
                    
                    # 取最近的消息（最多10轮）
                    recent = history[-20:] if len(history) > 20 else history
                    
                    lines = []
                    for msg in recent:
                        role = ""
                        content = ""
                        
                        if isinstance(msg, dict):
                            role = msg.get('role', 'unknown')
                            content = msg.get('content', '')
                            if isinstance(content, list):
                                # 消息链格式
                                text_parts = []
                                for part in content:
                                    if isinstance(part, dict):
                                        text_parts.append(part.get('text', ''))
                                    elif hasattr(part, 'text'):
                                        text_parts.append(part.text)
                                content = ' '.join(p for p in text_parts if p)
                        elif hasattr(msg, 'role'):
                            role = getattr(msg, 'role', 'unknown')
                            msg_content = getattr(msg, 'content', [])
                            if isinstance(msg_content, list):
                                text_parts = []
                                for part in msg_content:
                                    if hasattr(part, 'text'):
                                        text_parts.append(part.text)
                                    elif isinstance(part, dict):
                                        text_parts.append(part.get('text', ''))
                                content = ' '.join(p for p in text_parts if p)
                            elif isinstance(msg_content, str):
                                content = msg_content
                        
                        if content:
                            role_name = "用户" if role in ("user", "human") else ("助手" if role in ("assistant", "bot") else role)
                            lines.append(f"{role_name}: {content}")
                    
                    if lines:
                        logger.info(f"[Messenger] 获取到 {len(lines)} 条对话上下文 (UMO: {umo})")
                        return "\n".join(lines)
                
                except Exception as e:
                    logger.debug(f"[Messenger] 尝试 UMO {umo} 失败: {e}")
                    continue
            
            logger.debug(f"[Messenger] 未找到目标 {target_id} 的对话上下文")
            return ""
        
        except Exception as e:
            logger.debug(f"[Messenger] 获取对话上下文失败: {e}")
            return ""
    
    # ==================== 通告群聊 ====================
    
    async def _do_group_announce(self, event: AstrMessageEvent):
        """执行通告群聊"""
        message_str = event.message_str
        sender_id = str(event.get_sender_id())
        sender_name = await self._get_real_name(event, sender_id, event.get_sender_name())
        
        # 提取目标群号
        target_group = self._extract_target_group(message_str)
        
        if not target_group:
            yield event.plain_result(f"{self.error_prefix} 请指定目标群号。\n用法: 通告群聊 群号 消息内容")
            return
        
        # 检查 bot 是否在该群中
        in_group, group_name = await self._check_group(event, target_group)
        if not in_group:
            yield event.plain_result(f"{self.error_prefix} Bot 不在群 {target_group} 中，无法发送通告。")
            return
        
        # 提取消息内容（跳过命令头和群号）
        content = self._extract_all_content(event, skip_command=True)
        
        if not content:
            yield event.plain_result(f"{self.error_prefix} 请提供通告内容。\n用法: 通告群聊 群号 消息内容")
            return
        
        group_id = event.message_obj.group_id
        source_group_name = None if not group_id or self._is_inbox_group(group_id) else await self._get_group_name(event, str(group_id))
        sender_info = self._format_sender_info(sender_name, sender_id, source_group_name)
        
        announce_msg = f"{self.msg_prefix} {sender_info} 通告：\n{content}"
        
        logger.info(f"[Messenger] 通告群聊: {sender_name} -> 群{group_name}({target_group}): {content[:50]}...")
        
        msg_id = await self._send_group_message(event, target_group, announce_msg)
        if msg_id:
            self.message_records[msg_id] = {
                "from_user": sender_id,
                "to_user": sender_id,
                "from_name": sender_name,
                "to_name": sender_name,
                "original_msg_id": msg_id,
                "is_group_announce": True,
                "target_group": target_group,
                "target_group_name": group_name
            }
            self._trim_records()
            self._save_records()
            yield event.plain_result(f"{self.success_prefix} 已将通告发送到群「{group_name}」({target_group})！")
        else:
            yield event.plain_result(f"{self.error_prefix} 通告发送失败。")
    
    # ==================== 传话 ====================
    
    async def _do_tell(self, event: AstrMessageEvent):
        """执行传话"""
        message_str = event.message_str
        sender_id = str(event.get_sender_id())
        sender_name = await self._get_real_name(event, sender_id, event.get_sender_name())
        group_id = event.message_obj.group_id
        group_name = None if not group_id or self._is_inbox_group(group_id) else await self._get_group_name(event, str(group_id))
        
        target_qq = self._extract_target_qq(event, message_str)
        
        if not target_qq and self.enable_llm:
            llm_result = await self._llm_parse_tell_intent(message_str)
            if llm_result:
                target_qq = llm_result[0]
                logger.info(f"[Messenger] LLM 智能识别成功: 传话给 {target_qq}")
        
        if not target_qq:
            yield event.plain_result(f"{self.error_prefix} 请指定传话目标。\n用法: 传话 @某人 消息内容")
            return
        
        # 检查是否给 bot 自己传话
        try:
            if event.get_platform_name() == "aiocqhttp":
                from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
                if isinstance(event, AiocqhttpMessageEvent):
                    bot_info = await event.bot.api.call_action('get_login_info')
                    if target_qq == str(bot_info.get('user_id', '')):
                        yield event.plain_result("🤔 让我给我自己传话？有什么话直接跟我说不就好了~")
                        return
        except Exception:
            pass
        
        is_friend, friend_name = await self._check_friend(event, target_qq)
        if not is_friend:
            yield event.plain_result(f"{self.error_prefix} {target_qq} 不在我的好友列表中。")
            return
        
        content = self._extract_all_content(event, skip_command=True)
        
        if not content:
            content = "[空消息]"
        
        logger.info(f"[Messenger] 传话: {sender_name} -> {friend_name}: {content[:50]}...")
        
        sender_info = self._format_sender_info(sender_name, sender_id, group_name)
        tell_message = f"{self.msg_prefix} {sender_info} 对你说：\n{content}"
        
        via_inbox = self.enable_inbox and self.inbox_id and self.owner_qq and str(target_qq) == str(self.owner_qq)
        
        msg_id = await self._send_to_user(event, target_qq, tell_message)
        if msg_id:
            self.message_records[msg_id] = {
                "from_user": sender_id,
                "to_user": target_qq,
                "from_name": sender_name,
                "to_name": friend_name or target_qq,
                "original_msg_id": msg_id,
                "via_inbox": via_inbox
            }
            self._trim_records()
            self.user_last_received[target_qq] = {
                "from_user": sender_id,
                "from_name": sender_name,
                "msg_id": msg_id,
                "via_inbox": via_inbox
            }
            self._save_records()
            yield event.plain_result(f"{self.success_prefix} 已将消息传达给 {friend_name or target_qq}！")
        else:
            yield event.plain_result(f"{self.error_prefix} 消息发送失败。")
    
    # ==================== 群发 ====================
    
    async def _do_broadcast(self, event: AstrMessageEvent):
        """执行群发（仅管理员）"""
        # 直接提取包含图片的所有内容，跳过命令头
        content = self._extract_all_content(event, skip_command=True)
        
        if not content:
            yield event.plain_result(f"{self.error_prefix} 请提供要群发的消息内容。\n用法: 群发 消息内容")
            return
        
        try:
            if event.get_platform_name() != "aiocqhttp":
                yield event.plain_result(f"{self.error_prefix} 群发功能仅支持 QQ 平台。")
                return
            
            from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
            if not isinstance(event, AiocqhttpMessageEvent):
                yield event.plain_result(f"{self.error_prefix} 无法获取平台客户端。")
                return
            
            client = event.bot
            sender_id = str(event.get_sender_id())
            sender_name = await self._get_real_name(event, sender_id, event.get_sender_name())
            current_group_id = str(event.message_obj.group_id) if event.message_obj.group_id else ""
            
            group_name = None if not current_group_id or self._is_inbox_group(current_group_id) else await self._get_group_name(event, current_group_id)
            sender_info = self._format_sender_info(sender_name, sender_id, group_name)
            
            friend_list = await client.api.call_action('get_friend_list')
            group_list = await client.api.call_action('get_group_list')
            
            if not friend_list and not group_list:
                yield event.plain_result(f"{self.error_prefix} 好友列表和群列表都为空。")
                return
            
            friend_send_list = []
            excluded_current = 0
            inbox_excluded = 0
            for friend in friend_list:
                qq = str(friend.get('user_id', ''))
                if not qq or qq in self.broadcast_blacklist:
                    continue
                if not current_group_id and qq == sender_id:
                    excluded_current += 1
                    continue
                friend_send_list.append({'qq': qq, 'nickname': friend.get('nickname', qq)})
            
            group_send_list = []
            for group in group_list:
                gid = str(group.get('group_id', ''))
                if not gid or gid in self.broadcast_blacklist:
                    continue
                if current_group_id and gid == current_group_id:
                    excluded_current += 1
                    continue
                if self.enable_inbox and self.inbox_type == 'group' and self.inbox_id and gid == self.inbox_id:
                    inbox_excluded += 1
                    continue
                group_send_list.append({'group_id': gid, 'group_name': group.get('group_name', gid)})
            
            total = len(friend_send_list) + len(group_send_list)
            if total == 0:
                yield event.plain_result(f"{self.error_prefix} 没有可发送的目标。")
                return
            
            blacklist_excluded = len(friend_list) + len(group_list) - total - excluded_current - inbox_excluded
            inbox_info = f"\n📥 收件箱已排除: {inbox_excluded}" if inbox_excluded > 0 else ""
            yield event.plain_result(f"📢 开始群发...\n👤 好友: {len(friend_send_list)}\n👥 群聊: {len(group_send_list)}\n🚫 黑名单: {blacklist_excluded}\n🔇 当前会话: {excluded_current}{inbox_info}")
            
            success_count = 0
            fail_count = 0
            
            broadcast_msg = f"{self.msg_prefix} {sender_info} 对你说：\n{content}"
            
            for friend in friend_send_list:
                try:
                    msg_id = await self._send_to_user(event, friend['qq'], broadcast_msg)
                    if msg_id:
                        self.message_records[msg_id] = {
                            "from_user": sender_id,
                            "to_user": friend['qq'],
                            "from_name": sender_name,
                            "to_name": friend['nickname'],
                            "original_msg_id": msg_id
                        }
                    success_count += 1
                except Exception:
                    fail_count += 1
                if self.broadcast_delay > 0:
                    await asyncio.sleep(self.broadcast_delay)
            
            for group in group_send_list:
                try:
                    result = await client.api.call_action('send_group_msg', group_id=int(group['group_id']), message=broadcast_msg)
                    msg_id = str(result.get('message_id', '')) if result else None
                    if msg_id:
                        self.message_records[msg_id] = {
                            "from_user": sender_id,
                            "to_user": group['group_id'],
                            "from_name": sender_name,
                            "to_name": group['group_name'],
                            "original_msg_id": msg_id,
                            "is_group": True
                        }
                    success_count += 1
                except Exception:
                    fail_count += 1
                if self.broadcast_delay > 0:
                    await asyncio.sleep(self.broadcast_delay)
            
            self._trim_records()
            self._save_records()
            yield event.plain_result(f"{self.success_prefix} 群发完成！\n✅ 成功: {success_count}\n❌ 失败: {fail_count}")
            
        except Exception as e:
            logger.error(f"群发功能出错: {e}")
            yield event.plain_result(f"{self.error_prefix} 群发失败: {str(e)}")
    
    async def terminate(self):
        """插件卸载时保存并清理"""
        self._save_records()
        self.message_records.clear()
        self.user_last_received.clear()
