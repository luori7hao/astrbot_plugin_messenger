"""
通风报信插件 - 帮你传话给好友，支持来回对话
"""
import re
import json
import asyncio
from typing import Dict, Optional, Tuple
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api.message_components import Plain, At, Reply, Image
from astrbot.api import logger, AstrBotConfig

# 消息记录存储，用于追踪回复链（限制最大条数防止内存泄漏）
MAX_RECORDS = 500
message_records: Dict[str, dict] = {}
# 用户最近收到的传话记录
user_last_received: Dict[str, dict] = {}

def _trim_records():
    """当记录超过上限时清理最早的记录"""
    if len(message_records) > MAX_RECORDS:
        keys = list(message_records.keys())
        for k in keys[:len(keys) - MAX_RECORDS]:
            del message_records[k]

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
• `群发 消息内容` - 一键群发（管理员）

**【回复传话】**
引用传话消息，直接发送回复内容即可

**【注意事项】**
• 传话目标必须是 bot 的好友
• 支持发送图片，图文会一起转发
• 通告群聊和群发仅管理员可用"""
        
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
        
        logger.info(f"[Messenger] on_message: message_str='{(message_str or '')[:80]}', "
                    f"components={[type(c).__name__ for c in event.message_obj.message]}")
        
        # ========== 优先级1：引用传话消息 = 回复 ==========
        reply_msg_id = self._has_reply(event)
        if reply_msg_id:
            target_qq = None
            target_name = None
            is_group_reply = False
            
            if reply_msg_id in message_records:
                record = message_records[reply_msg_id]
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
                
                logger.info(f"[Messenger] 回复: {sender_name} -> {target_name}: {content[:50]}...")
                
                sender_info = self._format_sender_info(sender_name, sender_id, group_name)
                reply_msg = f"{self.msg_prefix} {sender_info} 让我回复你：\n{content}"
                
                # 回复始终发送到私聊（通过 _send_to_user 支持收件箱）
                new_msg_id = await self._send_to_user(event, target_qq, reply_msg)
                if new_msg_id:
                    message_records[new_msg_id] = {
                        "from_user": sender_id,
                        "to_user": target_qq,
                        "from_name": sender_name,
                        "to_name": target_name,
                        "original_msg_id": new_msg_id
                    }
                    _trim_records()
                    user_last_received[target_qq] = {
                        "from_user": sender_id,
                        "from_name": sender_name,
                        "msg_id": new_msg_id
                    }
                    yield event.plain_result(f"{self.success_prefix} 已将你的回复转达给 {target_name}！")
                else:
                    yield event.plain_result(f"{self.error_prefix} 消息发送失败。")
                event.stop_event()
                return
        
        # ========== 优先级2：通告群聊命令（仅管理员） ==========
        if self._is_group_announce_command(message_str):
            if not self._is_admin(sender_id):
                yield event.plain_result(f"{self.error_prefix} 通告群聊功能仅管理员可用。请在插件配置中添加你的QQ号到管理员列表。")
                event.stop_event()
                return
            async for result in self._do_group_announce(event):
                yield result
            event.stop_event()
            return
        
        # ========== 优先级3：群发命令（仅管理员） ==========
        if self._is_broadcast_command(event):
            if not self._is_admin(sender_id):
                yield event.plain_result(f"{self.error_prefix} 群发功能仅管理员可用。请在插件配置中添加你的QQ号到管理员列表。")
                event.stop_event()
                return
            async for result in self._do_broadcast(event):
                yield result
            event.stop_event()
            return
        
        # ========== 优先级4：传话命令 ==========
        if self._is_tell_command(message_str):
            async for result in self._do_tell(event):
                yield result
            event.stop_event()
            return
    
    # ==================== 通告群聊 ====================
    
    async def _do_group_announce(self, event: AstrMessageEvent):
        """执行通告群聊"""
        message_str = event.message_str
        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name()
        
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
            message_records[msg_id] = {
                "from_user": sender_id,
                "to_user": sender_id,
                "from_name": sender_name,
                "to_name": sender_name,
                "original_msg_id": msg_id,
                "is_group_announce": True,
                "target_group": target_group,
                "target_group_name": group_name
            }
            _trim_records()
            yield event.plain_result(f"{self.success_prefix} 已将通告发送到群「{group_name}」({target_group})！")
        else:
            yield event.plain_result(f"{self.error_prefix} 通告发送失败。")
    
    # ==================== 传话 ====================
    
    async def _do_tell(self, event: AstrMessageEvent):
        """执行传话"""
        message_str = event.message_str
        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name()
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
            message_records[msg_id] = {
                "from_user": sender_id,
                "to_user": target_qq,
                "from_name": sender_name,
                "to_name": friend_name or target_qq,
                "original_msg_id": msg_id,
                "via_inbox": via_inbox
            }
            _trim_records()
            user_last_received[target_qq] = {
                "from_user": sender_id,
                "from_name": sender_name,
                "msg_id": msg_id,
                "via_inbox": via_inbox
            }
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
            sender_name = event.get_sender_name()
            sender_id = str(event.get_sender_id())
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
                        message_records[msg_id] = {
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
                        message_records[msg_id] = {
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
            
            yield event.plain_result(f"{self.success_prefix} 群发完成！\n✅ 成功: {success_count}\n❌ 失败: {fail_count}")
            
        except Exception as e:
            logger.error(f"群发功能出错: {e}")
            yield event.plain_result(f"{self.error_prefix} 群发失败: {str(e)}")
    
    async def terminate(self):
        """插件卸载时清理"""
        message_records.clear()
        user_last_received.clear()
