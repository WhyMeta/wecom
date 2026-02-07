/**
 * WeCom Target Resolver (企业微信目标解析器)
 *
 * 解析 OpenClaw 的 `to` 字段（原始目标字符串），将其转换为企业微信支持的具体接收对象。
 * 支持显式前缀 (party:, tag: 等)、多目标（竖线分隔）和基于规则的启发式推断。
 *
 * **关于"目标发送"与"消息记录"的对应关系 (Target vs Inbound):**
 * - **发送 (Outbound)**: 支持一对多广播 (Party/Tag)。
 *   例如发送给 `party:1`，消息会触达该部门下所有成员。
 * - **接收 (Inbound)**: 总是来自具体的 **用户 (User)** 或 **群聊 (Chat)**。
 *   当成员回复部门广播消息时，可以视为一个新的单聊会话或在该成员的现有单聊中回复。
 *   因此，Outbound Target (如 Party) 与 Inbound Source (User) 不需要也不可能 1:1 强匹配。
 *   广播是"发后即忘" (Fire-and-Forget) 的通知模式，而回复是具体的会话模式。
 */

export interface WecomTarget {
    touser?: string;
    toparty?: string;
    totag?: string;
    chatid?: string;
}

export interface ResolveResult {
    /** 是否成功解析 */
    success: boolean;
    /** 解析后的目标 */
    target?: WecomTarget;
    /** 错误信息（当 success 为 false 时） */
    error?: string;
    /** 原始输入 */
    raw: string;
}

/**
 * 规范化数字字符串，去除前导零用于长度判断
 * 例如: "01" -> "1", "001" -> "1", "123" -> "123"
 */
function normalizeNumericId(id: string): string {
    // 保留至少一位数字，去除前导零
    return id.replace(/^0+/, "") || "0";
}

/**
 * 解析单个目标字符串（不含竖线分隔符）
 * 支持显式前缀 (party:, tag:, user: 等) 和启发式推断
 */
function resolveSingleTarget(segment: string): WecomTarget {
    const clean = segment.trim();
    if (!clean) return {};

    // 1. 显式类型前缀
    if (/^party:/i.test(clean)) {
        return { toparty: clean.replace(/^party:/i, "").trim() };
    }
    if (/^dept:/i.test(clean)) {
        return { toparty: clean.replace(/^dept:/i, "").trim() };
    }
    if (/^tag:/i.test(clean)) {
        return { totag: clean.replace(/^tag:/i, "").trim() };
    }
    if (/^group:/i.test(clean)) {
        return { chatid: clean.replace(/^group:/i, "").trim() };
    }
    if (/^chat:/i.test(clean)) {
        return { chatid: clean.replace(/^chat:/i, "").trim() };
    }
    if (/^user:/i.test(clean)) {
        return { touser: clean.replace(/^user:/i, "").trim() };
    }

    // 2. 启发式推断（无前缀时）

    // 群聊 ID 通常以 'wr' (外部群) 或 'wc' 开头
    if (/^(wr|wc)/i.test(clean)) {
        return { chatid: clean };
    }

    // 纯数字：去除前导零后，1-2位视为部门，3位及以上视为用户
    if (/^\d+$/.test(clean)) {
        const normalized = normalizeNumericId(clean);
        if (normalized.length < 3) {
            return { toparty: clean };
        }
        return { touser: clean };
    }

    // 默认为用户
    return { touser: clean };
}

/**
 * Parses a raw target string into a WecomTarget object.
 * 解析原始目标字符串为 WecomTarget 对象。
 *
 * 逻辑:
 * 1. 移除标准命名空间前缀 (wecom:, qywx: 等)。
 * 2. 检查显式类型前缀 (party:, tag:, group:, user:)。
 * 3. 支持多目标（竖线 | 分隔，如 user:zhangsan|lisi）。
 * 4. 启发式回退 (无前缀时):
 *    - 以 "wr" 或 "wc" 开头 -> Chat ID (群聊)
 *    - 纯数字（去前导零后 1-2 位）-> Party ID (部门)，如 "1", "2", "01"
 *    - 纯数字（去前导零后 3 位及以上）-> User ID (用户)，如 "123", "00123"
 *    - 其他 -> User ID (用户)
 *
 * 注意: 企业微信 user_id 可能是纯字母(zhangsan)、纯数字(18617165136)或混合(ZhangSan5136)。
 *       如需发送给3位以上的部门，请使用显式前缀 "party:123"。
 *
 * @param raw - The raw target string (e.g. "party:1", "zhangsan", "wecom:wr123", "18617165136", "user:zhangsan|lisi")
 * @returns ResolveResult 解析结果，包含成功/失败状态和错误信息
 */
export function resolveWecomTarget(raw: string | undefined): ResolveResult {
    if (!raw?.trim()) {
        return { success: false, error: "目标不能为空", raw: raw || "" };
    }

    // 1. 移除标准命名空间前缀
    let clean = raw.trim().replace(/^(wecom-agent|wecom|wechatwork|wework|qywx):/i, "");

    // 2. 显式类型前缀（支持多目标）
    if (/^party:/i.test(clean)) {
        const ids = clean.replace(/^party:/i, "").trim().split(/\s*\|\s*/);
        return { success: true, target: { toparty: ids.join("|") }, raw };
    }
    if (/^dept:/i.test(clean)) {
        const ids = clean.replace(/^dept:/i, "").trim().split(/\s*\|\s*/);
        return { success: true, target: { toparty: ids.join("|") }, raw };
    }
    if (/^tag:/i.test(clean)) {
        const ids = clean.replace(/^tag:/i, "").trim().split(/\s*\|\s*/);
        return { success: true, target: { totag: ids.join("|") }, raw };
    }
    if (/^group:/i.test(clean)) {
        const ids = clean.replace(/^group:/i, "").trim().split(/\s*\|\s*/);
        return { success: true, target: { chatid: ids.join("|") }, raw };
    }
    if (/^chat:/i.test(clean)) {
        const ids = clean.replace(/^chat:/i, "").trim().split(/\s*\|\s*/);
        return { success: true, target: { chatid: ids.join("|") }, raw };
    }
    if (/^user:/i.test(clean)) {
        const ids = clean.replace(/^user:/i, "").trim().split(/\s*\|\s*/);
        return { success: true, target: { touser: ids.join("|") }, raw };
    }

    // 3. 无前缀：检查是否包含竖线分隔符（多目标）
    if (/\|/.test(clean)) {
        const parts = clean.split(/\s*\|\s*/);
        const targets: WecomTarget[] = parts.map(resolveSingleTarget);

        // 合并相同类型的目标
        const merged: WecomTarget = {};
        const tousers: string[] = [];
        const topartys: string[] = [];
        const totags: string[] = [];
        const chatids: string[] = [];

        for (const t of targets) {
            if (t.touser) tousers.push(t.touser);
            if (t.toparty) topartys.push(t.toparty);
            if (t.totag) totags.push(t.totag);
            if (t.chatid) chatids.push(t.chatid);
        }

        if (tousers.length) merged.touser = tousers.join("|");
        if (topartys.length) merged.toparty = topartys.join("|");
        if (totags.length) merged.totag = totags.join("|");
        if (chatids.length) merged.chatid = chatids.join("|");

        return { success: true, target: merged, raw };
    }

    // 4. 单目标启发式解析
    return { success: true, target: resolveSingleTarget(clean), raw };
}

/**
 * 向后兼容的简化接口
 * 解析失败时返回 undefined（不抛出错误）
 * @deprecated 建议使用 resolveWecomTarget 获取完整结果
 */
export function resolveWecomTargetSimple(raw: string | undefined): WecomTarget | undefined {
    const result = resolveWecomTarget(raw);
    return result.success ? result.target : undefined;
}
