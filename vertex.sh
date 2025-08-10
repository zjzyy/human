#!/bin/bash
# 优化的 GCP API 密钥管理工具
# 支持 Gemini API 和 Vertex AI
# 版本: 2.0.0

# 仅启用 errtrace (-E) 与 nounset (-u)
set -Euo

# ===== 颜色定义 =====
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# ===== 全局配置 =====
# 版本信息
VERSION="8.8.8"
LAST_UPDATED="2025-08-10"

# 通用配置
PROJECT_PREFIX="${PROJECT_PREFIX:-gemini-key}"
MAX_RETRY_ATTEMPTS="${MAX_RETRY:-3}"
MAX_PARALLEL_JOBS="${CONCURRENCY:-20}"
TEMP_DIR=""  # 将在初始化时设置

# S3配置（新增）
S3_ENDPOINT=""
S3_ACCESS_KEY=""
S3_SECRET_KEY=""
S3_BUCKET=""
S3_DIRECTORY=""
S3_ENABLED=false  # S3功能是否可用

# Gemini模式配置
TIMESTAMP=$(date +%s)
# 改进的随机字符生成（兼容性更好）
if command -v openssl &>/dev/null; then
    RANDOM_CHARS=$(openssl rand -hex 2)
else
    RANDOM_CHARS=$(( RANDOM % 10000 ))
fi
EMAIL_USERNAME="momo${RANDOM_CHARS}${TIMESTAMP:(-4)}"
GEMINI_TOTAL_PROJECTS=175
PURE_KEY_FILE="key.txt"
COMMA_SEPARATED_KEY_FILE="comma_separated_keys_${EMAIL_USERNAME}.txt"
AGGREGATED_KEY_FILE="aggregated_verbose_keys_${EMAIL_USERNAME}.txt"
DELETION_LOG="project_deletion_$(date +%Y%m%d_%H%M%S).log"
CLEANUP_LOG="api_keys_cleanup_$(date +%Y%m%d_%H%M%S).log"

# Vertex模式配置
BILLING_ACCOUNT="${BILLING_ACCOUNT:-}"
VERTEX_PROJECT_PREFIX="${VERTEX_PROJECT_PREFIX:-vertex}"
MAX_PROJECTS_PER_ACCOUNT=${MAX_PROJECTS_PER_ACCOUNT:-3}
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-vertex-admin}"
KEY_DIR="${KEY_DIR:-./keys}"
ENABLE_EXTRA_ROLES=("roles/iam.serviceAccountUser" "roles/aiplatform.user")

# ===== 初始化 =====
# 禁用历史记录功能（从一开始就防止记录）
set +o history 2>/dev/null || true
# 使用安全的方式处理可能未定义的变量
[ -n "${HISTFILE:-}" ] && unset HISTFILE 2>/dev/null || true
export HISTSIZE=0 2>/dev/null || true
export HISTFILESIZE=0 2>/dev/null || true

# 创建唯一的临时目录
TEMP_DIR=$(mktemp -d -t gcp_script_XXXXXX) || {
    echo "错误：无法创建临时目录"
    exit 1
}

# 创建密钥目录
mkdir -p "$KEY_DIR" 2>/dev/null || {
    echo "错误：无法创建密钥目录 $KEY_DIR"
    exit 1
}
chmod 700 "$KEY_DIR" 2>/dev/null || true

# 开始计时
SECONDS=0

# ===== 日志函数（带颜色） =====
log() { 
    local level="${1:-INFO}"
    local msg="${2:-}"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "$level" in
        "INFO")     echo -e "${CYAN}[${timestamp}] [INFO] ${msg}${NC}" >&2 ;;
        "SUCCESS")  echo -e "${GREEN}[${timestamp}] [SUCCESS] ${msg}${NC}" >&2 ;;
        "WARN")     echo -e "${YELLOW}[${timestamp}] [WARN] ${msg}${NC}" >&2 ;;
        "ERROR")    echo -e "${RED}[${timestamp}] [ERROR] ${msg}${NC}" >&2 ;;
        *)          echo "[${timestamp}] [${level}] ${msg}" >&2 ;;
    esac
}

# ===== S3 相关函数（新增） =====

# 检查S3配置
check_s3_config() {
    if [ -z "$S3_ENDPOINT" ] || [ -z "$S3_ACCESS_KEY" ] || [ -z "$S3_SECRET_KEY" ] || [ -z "$S3_BUCKET" ]; then
        return 1
    fi
    return 0
}

# 检查S3上传工具的可用性（优先使用Google Cloud Shell内置工具）
ensure_s3_tool() {
    # 检查是否有s3cmd（Google Cloud Shell内置）
    if command -v s3cmd &>/dev/null; then
        log "SUCCESS" "找到s3cmd工具（Google Cloud Shell内置）"
        return 0
    fi
    
    # 检查是否有rclone（也是常见的S3工具）
    if command -v rclone &>/dev/null; then
        log "SUCCESS" "找到rclone工具"
        return 0
    fi
    
    # 检查AWS CLI
    if command -v aws &>/dev/null; then
        log "SUCCESS" "找到AWS CLI工具"
        return 0
    fi
    
    # 尝试安装AWS CLI（最后备选）
    log "WARN" "未找到S3上传工具，尝试安装AWS CLI..."
    if command -v python3 &>/dev/null; then
        if python3 -m pip install --user awscli --quiet 2>/dev/null; then
            log "SUCCESS" "AWS CLI安装成功"
            export PATH="$HOME/.local/bin:$PATH"
            return 0
        fi
    fi
    
    log "ERROR" "无法找到或安装S3上传工具"
    return 1
}

# 检查S3功能是否可用（包括配置和AWS CLI）
check_s3_available() {
    if [ "$S3_ENABLED" = "true" ] && check_s3_config; then
        return 0
    fi
    return 1
}

# 生成S3目录名（如果未指定）
generate_s3_directory() {
    if [ -z "$S3_DIRECTORY" ]; then
        S3_DIRECTORY=$(date +%Y%m%d)
        log "INFO" "使用默认S3目录: ${S3_DIRECTORY}"
    fi
}

# ===== Google Cloud Storage 备用方案 =====

# 上传文件到Google Cloud Storage（备用方案）
upload_to_gcs() {
    local local_file="$1"
    local gcs_bucket="$2"
    local gcs_key="$3"
    
    if [ ! -f "$local_file" ]; then
        log "ERROR" "文件不存在: ${local_file}"
        return 1
    fi
    
    if [ -z "$gcs_bucket" ] || [ -z "$gcs_key" ]; then
        log "ERROR" "GCS存储桶或文件名为空"
        return 1
    fi
    
    log "INFO" "上传文件到Google Cloud Storage: gs://${gcs_bucket}/${gcs_key}"
    
    if gsutil cp "$local_file" "gs://${gcs_bucket}/${gcs_key}" 2>/dev/null; then
        log "SUCCESS" "成功上传到GCS: ${gcs_key}"
        return 0
    else
        log "ERROR" "上传GCS失败: ${gcs_key}"
        return 1
    fi
}

# 创建GCS存储桶（如果不存在）
ensure_gcs_bucket() {
    local bucket_name="$1"
    
    if [ -z "$bucket_name" ]; then
        log "ERROR" "GCS存储桶名为空"
        return 1
    fi
    
    # 检查存储桶是否存在
    if gsutil ls -b "gs://${bucket_name}" &>/dev/null; then
        log "INFO" "GCS存储桶已存在: ${bucket_name}"
        return 0
    fi
    
    # 尝试创建存储桶
    log "INFO" "创建GCS存储桶: ${bucket_name}"
    local current_project
    current_project=$(gcloud config get-value project 2>/dev/null)
    
    if [ -z "$current_project" ]; then
        log "ERROR" "无法获取当前项目，请设置默认项目"
        return 1
    fi
    
    if gsutil mb -p "$current_project" "gs://${bucket_name}" 2>/dev/null; then
        log "SUCCESS" "GCS存储桶创建成功: ${bucket_name}"
        return 0
    else
        log "ERROR" "GCS存储桶创建失败: ${bucket_name}"
        return 1
    fi
}

# 智能上传函数（优先S3，备用GCS）
smart_upload() {
    local local_file="$1"
    local project_id="$2"
    local file_type="$3" # "json" 或 "key"
    
    if [ ! -f "$local_file" ]; then
        log "ERROR" "文件不存在: ${local_file}"
        return 1
    fi
    
    # 获取当前活动账号邮箱
    local current_email
    current_email=$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | head -n 1)
    
    if [ -z "$current_email" ]; then
        log "ERROR" "无法获取当前账号邮箱"
        return 1
    fi
    
    local file_extension
    case "$file_type" in
        "json") file_extension=".json" ;;
        "key") file_extension=".key" ;;
        *) file_extension="" ;;
    esac
    
    local base_filename="${current_email}-${project_id}${file_extension}"
    
    # 尝试S3上传
    if check_s3_available; then
        if upload_to_s3 "$local_file" "$base_filename"; then
            return 0
        else
            log "WARN" "S3上传失败，尝试GCS备用方案"
        fi
    fi
    
    # 尝试GCS备用方案
    if command -v gsutil &>/dev/null; then
        # 生成GCS存储桶名（基于项目ID或邮箱）
        local gcs_bucket_name
        gcs_bucket_name="vertex-keys-$(echo "$current_email" | cut -d'@' -f1 | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g')"
        
        # 确保存储桶存在
        if ensure_gcs_bucket "$gcs_bucket_name"; then
            local gcs_key="${S3_DIRECTORY:-$(date +%Y%m%d)}/${base_filename}"
            if upload_to_gcs "$local_file" "$gcs_bucket_name" "$gcs_key"; then
                log "INFO" "文件已上传到GCS作为备用: gs://${gcs_bucket_name}/${gcs_key}"
                return 0
            fi
        fi
    else
        log "WARN" "gsutil不可用，无法使用GCS备用方案"
    fi
    
    log "WARN" "所有上传方案都失败，文件仅保存在本地"
    return 1
}

# 上传文件到S3（智能选择工具）
upload_to_s3() {
    local local_file="$1"
    local s3_key="$2"
    
    if ! check_s3_available; then
        log "WARN" "S3功能不可用，跳过上传"
        return 1
    fi
    
    local s3_url="s3://${S3_BUCKET}/${S3_DIRECTORY}/${s3_key}"
    log "INFO" "上传文件到S3: ${s3_url}"
    
    # 方法1: 使用s3cmd（Google Cloud Shell内置）
    if command -v s3cmd &>/dev/null; then
        log "INFO" "使用s3cmd上传..."
        # 配置s3cmd
        local s3cmd_config="${TEMP_DIR}/s3cmd.cfg"
        cat > "$s3cmd_config" << EOF
[default]
access_key = ${S3_ACCESS_KEY}
secret_key = ${S3_SECRET_KEY}
host_base = ${S3_ENDPOINT#https://}
host_bucket = ${S3_ENDPOINT#https://}
use_https = True
EOF
        
        if s3cmd -c "$s3cmd_config" put "$local_file" "$s3_url" --quiet 2>/dev/null; then
            log "SUCCESS" "成功上传到S3: ${s3_key}"
            rm -f "$s3cmd_config"
            return 0
        else
            log "ERROR" "s3cmd上传失败: ${s3_key}"
            rm -f "$s3cmd_config"
        fi
    fi
    
    # 方法2: 使用rclone
    if command -v rclone &>/dev/null; then
        log "INFO" "使用rclone上传..."
        local rclone_config="${TEMP_DIR}/rclone.conf"
        cat > "$rclone_config" << EOF
[s3remote]
type = s3
provider = Other
access_key_id = ${S3_ACCESS_KEY}
secret_access_key = ${S3_SECRET_KEY}
endpoint = ${S3_ENDPOINT}
EOF
        
        if rclone --config "$rclone_config" copy "$local_file" "s3remote:${S3_BUCKET}/${S3_DIRECTORY}/" --quiet 2>/dev/null; then
            log "SUCCESS" "成功上传到S3: ${s3_key}"
            rm -f "$rclone_config"
            return 0
        else
            log "ERROR" "rclone上传失败: ${s3_key}"
            rm -f "$rclone_config"
        fi
    fi
    
    # 方法3: 使用AWS CLI（备用）
    if command -v aws &>/dev/null; then
        log "INFO" "使用AWS CLI上传..."
        # 临时设置AWS凭证
        export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
        export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
        export AWS_ENDPOINT_URL="$S3_ENDPOINT"
        
        if aws s3 cp "$local_file" "$s3_url" 2>/dev/null; then
            log "SUCCESS" "成功上传到S3: ${s3_key}"
            # 清理环境变量
            unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_URL 2>/dev/null || true
            return 0
        else
            log "ERROR" "AWS CLI上传失败: ${s3_key}"
            # 清理环境变量
            unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_URL 2>/dev/null || true
        fi
    fi
    
    log "ERROR" "所有S3上传方法都失败了"
    return 1
}

# 上传JSON密钥到S3
upload_json_key_to_s3() {
    local json_file="$1"
    local project_id="$2"
    
    smart_upload "$json_file" "$project_id" "json"
}

# 上传API Key到S3
upload_api_key_to_s3() {
    local api_key="$1"
    local project_id="$2"
    
    if [ -z "$api_key" ]; then
        log "ERROR" "API Key为空"
        return 1
    fi
    
    # 创建临时文件保存API Key
    local temp_key_file="${TEMP_DIR}/temp-api-key-${project_id}.key"
    echo "$api_key" > "$temp_key_file"
    
    local result=0
    smart_upload "$temp_key_file" "$project_id" "key" || result=1
    
    # 清理临时文件
    rm -f "$temp_key_file"
    
    return $result
}

# 彻底清空所有历史记录
clear_all_history() {
    log "INFO" "开始清空所有历史记录..."
    
    local cleared=false
    
    # 1. 清空当前shell会话的内存历史
    history -c 2>/dev/null || true
    
    # 2. 清空所有常见的历史文件
    local history_files=(
        "$HOME/.bash_history"
        "$HOME/.zsh_history"
        "$HOME/.sh_history"
    )
    
    # 安全地添加HISTFILE（如果已定义）
    if [ -n "${HISTFILE:-}" ]; then
        history_files+=("$HISTFILE")
    fi
    
    for hist_file in "${history_files[@]}"; do
        if [ -n "$hist_file" ] && [ -f "$hist_file" ] && [ -w "$hist_file" ]; then
            # 备份原始历史文件（以防需要恢复）
            cp "$hist_file" "${hist_file}.bak.$(date +%Y%m%d%H%M%S)" 2>/dev/null || true
            
            # 彻底清空历史文件
            > "$hist_file" 2>/dev/null && {
                cleared=true
                log "SUCCESS" "已清空历史文件: ${hist_file}"
            } || {
                # 如果直接清空失败，尝试删除后重建
                rm -f "$hist_file" 2>/dev/null && touch "$hist_file" 2>/dev/null && {
                    cleared=true
                    log "SUCCESS" "已重建历史文件: ${hist_file}"
                }
            }
        fi
    done
    
    # 3. 对于bash，尝试清空历史列表
    if [ -n "${BASH_VERSION:-}" ]; then
        # 保存原始值
        local orig_histsize="${HISTSIZE:-1000}"
        local orig_histfilesize="${HISTFILESIZE:-2000}"
        
        # 清空bash历史列表
        export HISTSIZE=0
        export HISTFILESIZE=0
        history -c 2>/dev/null || true
        history -w 2>/dev/null || true
        
        # 恢复默认大小（但不加载历史）
        export HISTSIZE="$orig_histsize"
        export HISTFILESIZE="$orig_histfilesize"
    fi
    
    # 4. 对于zsh，尝试清空历史
    if [ -n "${ZSH_VERSION:-}" ]; then
        # 清空zsh历史
        fc -p 2>/dev/null || true
        fc -P 2>/dev/null || true
    fi
    
    # 5. 同步到磁盘，确保更改生效
    sync 2>/dev/null || true
    
    # 6. 创建一个标记文件，提示用户需要手动清理父shell历史
    local marker_file="${HOME}/.gcpJSON_history_cleared"
    echo "历史已在 $(date) 清空" > "$marker_file"
    
    if [ "$cleared" = "true" ]; then
        log "SUCCESS" "历史文件已清空"
        log "INFO" "注意：如果您在父shell中运行此脚本，请手动执行以下命令："
        echo -e "${YELLOW}history -c && history -w${NC}" >&2
    else
        log "WARN" "未找到可清空的历史文件或无写入权限"
    fi
    
    # 防止当前命令被记录到历史（使用安全的方式）
    [ -n "${HISTFILE:-}" ] && unset HISTFILE 2>/dev/null || true
    set +o history 2>/dev/null || true
}

# ===== 错误处理 =====
handle_error() {
    local exit_code=$?
    local line_no=$1
    
    # 忽略某些非严重错误
    case $exit_code in
        141)  # SIGPIPE
            return 0
            ;;
        130)  # Ctrl+C
            log "INFO" "用户中断操作"
            exit 130
            ;;
    esac
    
    # 记录错误
    log "ERROR" "在第 ${line_no} 行发生错误 (退出码 ${exit_code})"
    
    # 严重错误才终止
    if [ $exit_code -gt 1 ]; then
        log "ERROR" "发生严重错误，请检查日志"
        return $exit_code
    else
        log "WARN" "发生非严重错误，继续执行"
        return 0
    fi
}

# 设置错误处理
trap 'handle_error $LINENO' ERR

# ===== 清理函数 =====
cleanup_resources() {
    local exit_code=$?
    
    # 确保AWS环境变量被清理（最终保险）
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_URL 2>/dev/null || true
    
    # 清理临时文件
    if [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR" 2>/dev/null || true
        log "INFO" "已清理临时文件"
    fi
    
    # 无条件清空所有历史记录（确保不留痕迹）
    log "INFO" "执行完整历史记录清理..."
    clear_all_history
    
    # 如果是正常退出，显示感谢信息
    if [ $exit_code -eq 0 ]; then
        echo -e "\n${CYAN}感谢使用 GCP API 密钥管理工具${NC}"
        echo -e "${YELLOW}请记得检查并删除不需要的项目以避免额外费用${NC}"
        if check_s3_available; then
            echo -e "${GREEN}密钥已安全上传到S3存储${NC}"
        elif command -v gsutil &>/dev/null; then
            echo -e "${GREEN}密钥已上传到Google Cloud Storage${NC}"
        fi
        echo -e "${PURPLE}${BOLD}历史文件已清空${NC}"
        echo -e "${YELLOW}提示：要清空当前shell的历史，请手动执行：${NC}"
        echo -e "${GREEN}history -c && history -w${NC}"
    fi
}

# 设置退出处理
trap cleanup_resources EXIT

# ===== 工具函数 =====

# 改进的重试函数（支持命令）
retry() {
    local max_attempts="$MAX_RETRY_ATTEMPTS"
    local attempt=1
    local delay
    
    while [ $attempt -le $max_attempts ]; do
        if "$@"; then
            return 0
        fi
        
        local error_code=$?
        
        if [ $attempt -ge $max_attempts ]; then
            log "ERROR" "命令在 ${max_attempts} 次尝试后失败: $*"
            return $error_code
        fi
        
        delay=$(( attempt * 10 + RANDOM % 5 ))
        log "WARN" "重试 ${attempt}/${max_attempts}: $* (等待 ${delay}s)"
        sleep $delay
        attempt=$((attempt + 1)) || true
    done
}

# 检查命令是否存在
require_cmd() { 
    if ! command -v "$1" &>/dev/null; then
        log "ERROR" "缺少依赖: $1"
        exit 1
    fi
}

# 交互确认（支持非交互式环境）
ask_yes_no() {
    local prompt="$1"
    local default="${2:-N}"
    local resp
    
    # 非交互式环境
    if [ ! -t 0 ]; then
        if [[ "$default" =~ ^[Yy]$ ]]; then
            log "INFO" "非交互式环境，自动选择: 是"
            return 0
        else
            log "INFO" "非交互式环境，自动选择: 否"
            return 1
        fi
    fi
    
    # 交互式环境
    if [[ "$default" == "N" ]]; then
        read -r -p "${prompt} [y/N]: " resp || resp="$default"
    else
        read -r -p "${prompt} [Y/n]: " resp || resp="$default"
    fi
    
    resp=${resp:-$default}
    [[ "$resp" =~ ^[Yy]$ ]]
}

# 生成唯一后缀
unique_suffix() { 
    if command -v uuidgen &>/dev/null; then
        uuidgen | tr -d '-' | cut -c1-6 | tr '[:upper:]' '[:lower:]'
    else
        echo "$(date +%s%N 2>/dev/null || date +%s)${RANDOM}" | sha256sum | cut -c1-6
    fi
}

# 生成项目ID
new_project_id() {
    local prefix="${1:-$PROJECT_PREFIX}"
    
    # 优先使用邮箱用户名生成项目ID
    local active_account
    active_account=$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | head -n 1)
    
    local base_name
    if [ -n "$active_account" ]; then
        # 从邮箱提取用户名，并进行清理
        base_name=$(echo "$active_account" | cut -d'@' -f1 | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g' | sed 's/-*$//g')
    else
        # 如果无法获取邮箱，则使用传入的前缀或默认前缀
        base_name="$prefix"
    fi
    
    # 添加日期和唯一后缀
    local suffix
    suffix=$(date +%m%d)-$(unique_suffix)
    
    local full_id="${base_name}-${suffix}"
    
    # 清理并确保符合GCP项目ID规范 (6-30个字符, 小写字母, 数字, 连字符)
    # 最后的sed确保ID不以连字符结尾
    echo "$full_id" | sed -e 's/[^a-z0-9-]/-/g' -e 's/^-*//' | cut -c1-30 | sed 's/-*$//'
}

# 安全检测服务是否已启用
is_service_enabled() {
    local proj="$1"
    local svc="$2"
    
    gcloud services list --enabled --project="$proj" --filter="name:${svc}" --format='value(name)' 2>/dev/null | grep -q .
}

# 带错误处理的命令执行
safe_exec() {
    local output
    local status
    
    output=$("$@" 2>&1)
    status=$?
    
    if [ $status -ne 0 ]; then
        echo "$output" >&2
        return $status
    fi
    
    echo "$output"
    return 0
}

# 并行处理单个项目的函数
# @param $1: project_id
# @param $2: temp_dir
# @param $3: job_num
# @param $4: total_jobs
process_project() {
    local project_id="$1"
    local temp_dir="$2"
    local current_job_num="$3"
    local total_jobs="$4"

    # 将此作业的日志重定向到单独的文件以避免输出混乱
    local job_log="${temp_dir}/job_${project_id}.log"
    
    {
        log "INFO" "[并行任务 ${current_job_num}/${total_jobs}] 开始处理项目: ${project_id}"
        
        if ! enable_services "$project_id"; then
            log "ERROR" "为项目 ${project_id} 启用API失败。跳过此项目。"
            echo "${project_id}" >> "${temp_dir}/failed.txt"
            return 1
        fi

        local key_file_path
        key_file_path=$(vertex_setup_service_account "$project_id")
        
        if [ -n "$key_file_path" ] && [ -f "$key_file_path" ]; then
            log "SUCCESS" "成功为项目 ${project_id} 生成服务账号密钥。"
            
            # 上传JSON密钥到S3（如果配置了S3）
            if check_s3_available; then
                upload_json_key_to_s3 "$key_file_path" "$project_id"
            fi
            
            local api_key
            api_key=$(create_and_print_api_key "$project_id")
            if [ -n "$api_key" ]; then
                log "SUCCESS" "成功为项目 ${project_id} 生成API Key。"
                
                # 上传API Key到S3（如果配置了S3）
                if check_s3_available; then
                    upload_api_key_to_s3 "$api_key" "$project_id"
                fi
                
                # 将服务账号密钥路径和API Key在同一行写入，用':::'分隔
                echo "${key_file_path}:::${api_key}" >> "${temp_dir}/success.txt"
            else
                log "WARN" "为项目 ${project_id} 生成API Key失败，但服务账号密钥已生成。"
                # 即使API Key失败，也记录下成功的服务账号密钥
                echo "${key_file_path}:::" >> "${temp_dir}/success.txt"
            fi
        else
            log "ERROR" "为项目 ${project_id} 生成密钥失败。"
            echo "${project_id}" >> "${temp_dir}/failed.txt"
        fi
    } >> "$job_log" 2>&1 # 追加模式，以防重试时覆盖
}

# 检查环境
check_env() {
    log "INFO" "检查环境配置..."
    
    # 检查必要命令
    require_cmd gcloud
    
    # 检查 gcloud 配置
    if ! gcloud config list account --quiet &>/dev/null; then
        log "ERROR" "请先运行 'gcloud init' 初始化"
        exit 1
    fi
    
    # 检查登录状态
    local active_account
    active_account=$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null || true)
    
    if [ -z "$active_account" ]; then
        log "ERROR" "请先运行 'gcloud auth login' 登录"
        exit 1
    fi
    
    log "SUCCESS" "环境检查通过 (账号: ${active_account})"
}

# 配额检查（修复版）
check_quota() {
    log "INFO" "检查项目创建配额..."
    
    local current_project
    current_project=$(gcloud config get-value project 2>/dev/null || true)
    
    if [ -z "$current_project" ]; then
        log "WARN" "未设置默认项目，跳过配额检查"
        return 0
    fi
    
    local projects_quota=""
    local quota_output
    
    # 尝试获取配额（GA版本）
    if quota_output=$(gcloud services quota list \
        --service=cloudresourcemanager.googleapis.com \
        --consumer="projects/${current_project}" \
        --filter='metric=cloudresourcemanager.googleapis.com/project_create_requests' \
        --format=json 2>/dev/null); then
        
        projects_quota=$(echo "$quota_output" | grep -oP '"effectiveLimit":\s*"\K[^"]+' | head -n 1)
    fi
    
    # 如果GA版本失败，尝试Alpha版本
    if [ -z "$projects_quota" ]; then
        log "INFO" "尝试使用 alpha 命令获取配额..."
        
        if quota_output=$(gcloud alpha services quota list \
            --service=cloudresourcemanager.googleapis.com \
            --consumer="projects/${current_project}" \
            --filter='metric:cloudresourcemanager.googleapis.com/project_create_requests' \
            --format=json 2>/dev/null); then
            
            projects_quota=$(echo "$quota_output" | grep -oP '"INT64":\s*"\K[^"]+' | head -n 1)
        fi
    fi
    
    # 处理配额结果
    if [ -z "$projects_quota" ] || ! [[ "$projects_quota" =~ ^[0-9]+$ ]]; then
        log "WARN" "无法获取配额信息，将继续执行"
        if ! ask_yes_no "无法检查配额，是否继续？" "N"; then
            return 1
        fi
        return 0
    fi
    
    local quota_limit=$projects_quota
    log "INFO" "项目创建配额限制: ${quota_limit}"
    
    # 检查Gemini项目数量
    if [ "${GEMINI_TOTAL_PROJECTS:-0}" -gt "$quota_limit" ]; then
        log "WARN" "计划创建的项目数(${GEMINI_TOTAL_PROJECTS})超过配额(${quota_limit})"
        
        echo "请选择："
        echo "1. 继续尝试创建 ${GEMINI_TOTAL_PROJECTS} 个项目"
        echo "2. 调整为创建 ${quota_limit} 个项目"
        echo "3. 取消操作"
        
        local choice
        read -r -p "请选择 [1-3]: " choice
        
        case "$choice" in
            1) log "INFO" "将尝试创建 ${GEMINI_TOTAL_PROJECTS} 个项目" ;;
            2) GEMINI_TOTAL_PROJECTS=$quota_limit
               log "INFO" "已调整为创建 ${GEMINI_TOTAL_PROJECTS} 个项目" ;;
            *) log "INFO" "操作已取消"
               return 1 ;;
        esac
    fi
    
    return 0
}

# 启用服务API
enable_services() {
    local proj="$1"
    shift
    
    local services=("$@")
    
    # 如果没有指定服务，使用默认列表
    if [ ${#services[@]} -eq 0 ]; then
        services=(
            "aiplatform.googleapis.com"
            "iam.googleapis.com"
            "iamcredentials.googleapis.com"
            "cloudresourcemanager.googleapis.com"
        )
    fi
    
    log "INFO" "为项目 ${proj} 批量启用 ${#services[@]} 个API服务..."

    if retry gcloud services enable "${services[@]}" --project="$proj" --quiet; then
        log "SUCCESS" "成功为项目 ${proj} 批量启用服务"
        return 0
    else
        log "ERROR" "为项目 ${proj} 批量启用服务失败"
        return 1
    fi
}

# 进度条显示
show_progress() {
    local completed="${1:-0}"
    local total="${2:-1}"
    
    # 参数验证
    if [ "$total" -le 0 ]; then
        return
    fi
    
    # 确保不超过总数
    if [ "$completed" -gt "$total" ]; then
        completed=$total
    fi
    
    # 计算百分比
    local percent=$((completed * 100 / total))
    local bar_length=50
    local filled=$((percent * bar_length / 100))
    
    # 生成进度条 - 使用安全的方式循环
    local bar=""
    local i=0
    while [ $i -lt $filled ]; do
        bar+="█"
        i=$((i + 1)) || true
    done
    
    i=$filled
    while [ $i -lt $bar_length ]; do
        bar+="░"
        i=$((i + 1)) || true
    done
    
    # 显示进度
    printf "\r[%s] %3d%% (%d/%d)" "$bar" "$percent" "$completed" "$total"
    
    # 完成时换行
    if [ "$completed" -eq "$total" ]; then
        echo
    fi
}

# JSON解析（改进版本）
parse_json() {
    local json="$1"
    local field="$2"
    
    if [ -z "$json" ]; then
        log "ERROR" "JSON解析: 输入为空"
        return 1
    fi
    
    # 尝试使用 jq（如果可用）
    if command -v jq &>/dev/null; then
        local result
        result=$(echo "$json" | jq -r "$field" 2>/dev/null)
        if [ -n "$result" ] && [ "$result" != "null" ]; then
            echo "$result"
            return 0
        fi
    fi
    
    # 备用方法 - 针对keyString专门处理
    if [ "$field" = ".keyString" ]; then
        local value
        # 尝试多种模式匹配
        value=$(echo "$json" | grep -o '"keyString":"[^"]*"' | sed 's/"keyString":"//;s/"$//' | head -n 1)
        
        if [ -z "$value" ]; then
            # 第二种尝试
            value=$(echo "$json" | grep -o '"keyString" *: *"[^"]*"' | sed 's/"keyString" *: *"//;s/"$//' | head -n 1)
        fi
        
        if [ -n "$value" ]; then
            echo "$value"
            return 0
        fi
    fi
    
    # 通用字段处理
    local field_name
    field_name=$(echo "$field" | sed 's/^\.//; s/\[[0-9]*\]//g')
    local value
    value=$(echo "$json" | grep -o "\"$field_name\":[^,}]*" | sed "s/\"$field_name\"://;s/\"//g;s/^ *//;s/ *$//" | head -n 1)
    
    if [ -n "$value" ] && [ "$value" != "null" ]; then
        echo "$value"
        return 0
    fi
    
    log "WARN" "JSON解析: 无法提取字段 $field"
    return 1
}

# 新增函数：创建并打印 API Key
create_and_print_api_key() {
    local project_id="$1"
    log "INFO" "为项目 ${project_id} 创建 API Key..."

    # 启用 generativelanguage.googleapis.com API
    if ! is_service_enabled "$project_id" "generativelanguage.googleapis.com"; then
        log "INFO" "启用 generativelanguage.googleapis.com API..."
        if ! retry gcloud services enable "generativelanguage.googleapis.com" --project="$project_id" --quiet; then
            log "ERROR" "启用 generativelanguage.googleapis.com API 失败"
            return 1
        fi
    fi

    local key_json
    key_json=$(gcloud alpha services api-keys create \
        --project="$project_id" \
        --display-name="AI Studio Key" \
        --format=json 2>/dev/null)

    if [ -z "$key_json" ]; then
        log "ERROR" "创建 API Key 失败，未收到gcloud的返回信息。"
        return 1
    fi

    local key_string
    key_string=$(parse_json "$key_json" ".keyString")

    if [ -n "$key_string" ]; then
        log "SUCCESS" "成功创建 API Key"
        echo "$key_string"
        return 0
    else
        log "ERROR" "无法从 gcloud 返回的 JSON 中解析出 API Key"
        log "ERROR" "gcloud 返回内容: $key_json"
        return 1
    fi
}

# 清理旧的服务账号密钥
clean_old_service_account_keys() {
    local sa_email="$1"
    local project_id="$2"
    local count_to_delete="${3:-1}"
    
    log "INFO" "开始清理服务账号 ${sa_email} 的旧密钥（删除 ${count_to_delete} 个）"
    
    # 获取所有密钥，按创建时间排序（最旧的在前）
    local key_ids
    key_ids=$(gcloud iam service-accounts keys list \
        --iam-account="$sa_email" \
        --project="$project_id" \
        --format='value(name)' \
        --sort-by='validAfterTime' 2>/dev/null)
    
    if [ -z "$key_ids" ]; then
        log "ERROR" "无法获取服务账号密钥列表"
        return 1
    fi
    
    # 转换为数组，跳过Google管理的密钥（通常以项目ID结尾）
    local deletable_keys=()
    while IFS= read -r key_id; do
        if [ -n "$key_id" ]; then
            # 跳过Google管理的密钥（这些密钥ID通常较短且以项目ID结尾）
            # 更宽松的匹配模式，允许更多格式的密钥ID
            if [[ "$key_id" =~ projects/.*/serviceAccounts/.*/keys/[0-9a-f]+$ ]]; then
                deletable_keys+=("$key_id")
            fi
        fi
    done <<< "$key_ids"
    
    local available_keys=${#deletable_keys[@]}
    
    if [ "$available_keys" -eq 0 ]; then
        log "WARN" "没有找到可删除的用户创建的密钥"
        return 1
    fi
    
    if [ "$count_to_delete" -gt "$available_keys" ]; then
        log "WARN" "要删除的密钥数(${count_to_delete})超过可用密钥数(${available_keys})"
        count_to_delete=$available_keys
    fi
    
    log "INFO" "找到 ${available_keys} 个可删除的密钥，将删除最旧的 ${count_to_delete} 个"
    
    # 删除最旧的密钥
    local success=0
    local failed=0
    
    for ((i=0; i<count_to_delete; i++)); do
        local key_id="${deletable_keys[i]}"
        local key_name
        key_name=$(basename "$key_id")
        
        log "INFO" "删除密钥: ${key_name}"
        
        if gcloud iam service-accounts keys delete "$key_id" \
            --iam-account="$sa_email" \
            --project="$project_id" \
            --quiet 2>/dev/null; then
            log "SUCCESS" "成功删除密钥: ${key_name}"
            success=$((success + 1))
        else
            log "ERROR" "删除密钥失败: ${key_name}"
            failed=$((failed + 1))
        fi
    done
    
    log "INFO" "密钥清理完成: 成功删除 ${success} 个，失败 ${failed} 个"
    
    if [ $success -gt 0 ]; then
        return 0
    else
        return 1
    fi
}

# 写入密钥文件
write_keys_to_files() {
    local api_key="$1"
    
    if [ -z "$api_key" ]; then
        log "ERROR" "密钥为空，无法写入文件"
        return 1
    fi
    
    # 使用文件锁确保并发安全
    {
        flock -x 9
        
        # 写入纯密钥文件
        echo "$api_key" >> "$PURE_KEY_FILE"
        
        # 写入逗号分隔文件
        if [ -s "$COMMA_SEPARATED_KEY_FILE" ]; then
            echo -n "," >> "$COMMA_SEPARATED_KEY_FILE"
        fi
        echo -n "$api_key" >> "$COMMA_SEPARATED_KEY_FILE"
        
    } 9>"${TEMP_DIR}/keyfile.lock"
}


# ===== Vertex AI 相关函数 =====

# Vertex主菜单
vertex_main() {
    local num_to_process="${1:-1}" # 从参数获取数量，默认为1
    local start_time=$SECONDS
    
    echo -e "\n${CYAN}${BOLD}======================================================"
    echo -e "    Google Vertex AI 密钥管理工具"
    echo -e "======================================================${NC}\n"
    
    check_env || return 1
    
    # 检查S3配置和上传工具
    if check_s3_config; then
        log "INFO" "检测到S3配置，查找上传工具..."
        if ensure_s3_tool; then
            S3_ENABLED=true
            log "SUCCESS" "S3上传工具已就绪，密钥将自动上传到S3"
        else
            S3_ENABLED=false
            log "ERROR" "无法找到S3上传工具，密钥将仅保存在本地"
            log "INFO" "提示: Google Cloud Shell通常内置s3cmd工具"
        fi
    fi
    
    echo -e "${YELLOW}警告: Vertex AI 需要结算账户，会产生实际费用！${NC}\n"
    
    # 获取结算账户
    log "INFO" "检查结算账户..."
    local billing_accounts
    billing_accounts=$(gcloud billing accounts list --filter='open=true' --format='value(name,displayName)' 2>/dev/null || echo "")
    
    if [ -z "$billing_accounts" ]; then
        log "ERROR" "未找到任何开放的结算账户"
        echo -e "${RED}Vertex AI 需要有效的结算账户才能使用${NC}"
        return 1
    fi
    
    # 转换为数组
    local billing_array=()
    while IFS=$'\t' read -r id name; do
        billing_array+=("${id##*/} - $name")
    done <<< "$billing_accounts"
    
    local billing_count=${#billing_array[@]}
    
    # 选择结算账户
    if [ "$billing_count" -eq 1 ]; then
        BILLING_ACCOUNT="${billing_array[0]%% - *}"
        log "INFO" "使用结算账户: ${BILLING_ACCOUNT}"
    else
        echo "可用的结算账户:"
        for ((i=0; i<billing_count; i++)); do
            echo "$((i+1)). ${billing_array[i]}"
        done
        echo
        
        local acc_num
        read -r -p "请选择结算账户 [1-${billing_count}]: " acc_num
        
        if [[ "$acc_num" =~ ^[0-9]+$ ]] && [ "$acc_num" -ge 1 ] && [ "$acc_num" -le "$billing_count" ]; then
            BILLING_ACCOUNT="${billing_array[$((acc_num-1))]%% - *}"
            log "INFO" "选择结算账户: ${BILLING_ACCOUNT}"
        else
            log "ERROR" "无效的选择"
            return 1
        fi
    fi
    
    # 显示警告
    echo -e "\n${YELLOW}${BOLD}重要提醒:${NC}"
    echo -e "${YELLOW}• 使用 Vertex AI 将消耗 \$300 免费额度${NC}"
    echo -e "${YELLOW}• 超出免费额度后将产生实际费用${NC}"
    echo -e "${YELLOW}• 请确保已设置预算警报${NC}"
    echo
    
    echo -n -e "${YELLOW}已了解费用风险，按 Enter 继续或等待 3 秒... ${NC}"
    # -r: raw input, -t 3: timeout 3s.
    # Returns 0 if Enter is pressed, >0 on timeout.
    if read -r -t 3; then
      # User pressed Enter
      echo
    else
      # Timeout occurred
      echo
    fi
    
    # 自动查找与配置项目
    log "INFO" "快速查找与结算账户 ${BILLING_ACCOUNT} 关联的项目..."
    
    local billed_projects_str
    billed_projects_str=$(gcloud projects list \
        --filter="billingInfo.billingAccountName=billingAccounts/${BILLING_ACCOUNT} AND lifecycleState=ACTIVE" \
        --format="value(projectId)" 2>/dev/null)

    local billed_projects=()
    if [ -n "$billed_projects_str" ]; then
        # 使用 readarray 将输出按行读入数组
        readarray -t billed_projects <<< "$billed_projects_str"
    fi

    local existing_project_count=${#billed_projects[@]}
    log "INFO" "找到 ${existing_project_count} 个已关联的项目."

    # 如果项目少于所需数量，则创建新的
    local projects_to_create=0
    if [ "$existing_project_count" -lt "$num_to_process" ]; then
        projects_to_create=$((num_to_process - existing_project_count))
        log "INFO" "项目数量不足${num_to_process}个，将创建 ${projects_to_create} 个新项目."
    fi

    if [ "$projects_to_create" -gt 0 ]; then
        log "INFO" "开始创建和配置新项目..."
        local i=1
        while [ $i -le $projects_to_create ]; do
            local new_project_id
            new_project_id=$(new_project_id "$VERTEX_PROJECT_PREFIX")
            
            log "INFO" "[${i}/${projects_to_create}] 创建项目: ${new_project_id}"
            if ! retry gcloud projects create "$new_project_id" --quiet; then
                log "ERROR" "创建项目 ${new_project_id} 失败。中止操作。"
                return 1
            fi
            
            log "INFO" "关联结算账户到 ${new_project_id}..."
            retry gcloud billing projects link "$new_project_id" --billing-account="$BILLING_ACCOUNT" --quiet
            local exit_code=$?
            if [ ${exit_code} -ne 0 ]; then
                log "ERROR" "关联结算账户失败: ${new_project_id} (退出码: ${exit_code})"
                gcloud projects delete "$new_project_id" --quiet 2>/dev/null
                return 1
            fi
            
            billed_projects+=("$new_project_id")
            log "SUCCESS" "成功创建并关联项目: ${new_project_id}"
            i=$((i + 1))
        done
    fi
    
    # 选择所需数量的项目进行处理
    local projects_to_process=("${billed_projects[@]:0:${num_to_process}}")
    log "INFO" "将为以下 ${#projects_to_process[@]} 个项目并行生成Vertex AI密钥:"
    printf -- " - %s\n" "${projects_to_process[@]}"
    echo

    # 为这些项目生成密钥（并行处理）
    local generated_key_files=()
    local success_count=0
    local failure_count=0
    
    # 清理临时结果文件
    rm -f "${TEMP_DIR}/success.txt" "${TEMP_DIR}/failed.txt" "${TEMP_DIR}/job_"*.log
    touch "${TEMP_DIR}/success.txt" "${TEMP_DIR}/failed.txt"

    local current=0
    local total=${#projects_to_process[@]}

    for project_id in "${projects_to_process[@]}"; do
        current=$((current + 1))
        
        # 当后台任务达到最大并发数时，等待一个任务完成
        while (( $(jobs -p | wc -l) >= MAX_PARALLEL_JOBS )); do
            sleep 1
        done

        process_project "$project_id" "$TEMP_DIR" "$current" "$total" &
    done

    log "INFO" "所有任务已启动，等待全部完成..."
    wait

    # 从临时文件收集结果
    log "INFO" "所有任务完成，正在汇总并打印日志..."
    
    # 按顺序打印每个作业的日志
    for project_id in "${projects_to_process[@]}"; do
        local log_file="${TEMP_DIR}/job_${project_id}.log"
        if [ -f "$log_file" ]; then
            cat "$log_file"
        fi
    done

    # 读取成功记录
    local success_records=()
     while IFS= read -r line; do
        [ -n "$line" ] && success_records+=("$line")
    done < "${TEMP_DIR}/success.txt"

    local failed_projects=()
    while IFS= read -r project_id; do
        [ -n "$project_id" ] && failed_projects+=("$project_id")
    done < "${TEMP_DIR}/failed.txt"

    local success_count=${#success_records[@]}
    local failure_count=${#failed_projects[@]}

    # 打印结果和密钥内容
    echo
    log "INFO" "操作完成！成功: ${success_count}, 失败: ${failure_count}."

    if [ ${#success_records[@]} -gt 0 ]; then
        log "INFO" "打印生成的密钥内容:"
        for record in "${success_records[@]}"; do
            # 解析记录，格式为: /path/to/key.json:::API_KEY_STRING
            local sa_key_path
            local api_key
            sa_key_path=$(echo "$record" | cut -d: -f1-3) # 假定路径中可能也有冒号
            api_key=$(echo "$record" | cut -d: -f4-)
            
            # 修正解析方法
            sa_key_path=$(echo "$record" | awk -F':::' '{print $1}')
            api_key=$(echo "$record" | awk -F':::' '{print $2}')


            local proj_id
            proj_id=$(basename "$sa_key_path" | sed -E "s/(${VERTEX_PROJECT_PREFIX}|${PROJECT_PREFIX})-[a-z0-9]+-[a-z0-9]+.*/\0/" | sed -E "s/-${SERVICE_ACCOUNT_NAME}-.*//")

            echo -e "\n${PURPLE}${BOLD}===== 项目: ${proj_id} =====${NC}"

            # 打印服务账号JSON密钥
            if [ -n "$sa_key_path" ] && [ -f "$sa_key_path" ]; then
                echo -e "${CYAN}--- 服务账号 (JSON Key) ---${NC}"
                echo -e "路径: ${sa_key_path}"
                cat "$sa_key_path"
                echo
            fi

            # 打印API Key
            if [ -n "$api_key" ]; then
                echo -e "${CYAN}--- AI Studio (API Key) ---${NC}"
                echo -e "密钥: ${api_key}"
            fi

            echo -e "${PURPLE}${BOLD}========================================================================${NC}"
        done
    fi

    # 显示执行时间
    local duration=$((SECONDS - start_time))
    log "INFO" "操作完成，耗时: $((duration / 60))分$((duration % 60))秒"
}

# 配置Vertex服务账号
vertex_setup_service_account() {
    local project_id="$1"
    local sa_email="${SERVICE_ACCOUNT_NAME}@${project_id}.iam.gserviceaccount.com"
    
    # 检查服务账号是否存在
    if ! gcloud iam service-accounts describe "$sa_email" --project="$project_id" &>/dev/null; then
        log "INFO" "创建服务账号..."
        if ! retry gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
            --display-name="Vertex AI Service Account" \
            --project="$project_id" --quiet; then
            log "ERROR" "创建服务账号失败"
            return 1
        fi
    else
        log "INFO" "服务账号已存在"
    fi
    
    # 分配角色
    local roles=(
        "roles/aiplatform.admin"
        "roles/iam.serviceAccountUser"
        "roles/iam.serviceAccountTokenCreator"
        "roles/aiplatform.user"
    )
    
    log "INFO" "分配IAM角色..."
    for role in "${roles[@]}"; do
        if retry gcloud projects add-iam-policy-binding "$project_id" \
            --member="serviceAccount:${sa_email}" \
            --role="$role" \
            --quiet &>/dev/null; then
            log "SUCCESS" "授予角色: ${role}"
        else
            log "WARN" "授予角色失败: ${role}"
        fi
    done
    
    # 检查现有密钥数量
    log "INFO" "检查服务账号现有密钥数量..."
    local existing_keys_count
    existing_keys_count=$(gcloud iam service-accounts keys list \
        --iam-account="$sa_email" \
        --project="$project_id" \
        --format='value(name)' 2>/dev/null | wc -l)
    
    if [ "$existing_keys_count" -ge 10 ]; then
        log "ERROR" "服务账号 ${sa_email} 已有 ${existing_keys_count} 个密钥，已达到最大限制(10个)"
        log "INFO" "解决方案: 删除一些旧的密钥后重试"
        
        # 列出现有密钥
        log "INFO" "现有密钥列表:"
        gcloud iam service-accounts keys list \
            --iam-account="$sa_email" \
            --project="$project_id" \
            --format='table(name.basename(),validAfterTime,validBeforeTime)' 2>/dev/null || true
        
        # 提供自动清理选项
        if ask_yes_no "是否自动删除最旧的密钥？(删除1个最旧的密钥以腾出空间)" "N"; then
            clean_old_service_account_keys "$sa_email" "$project_id" 1
            if [ $? -eq 0 ]; then
                log "SUCCESS" "已清理旧密钥，现在可以创建新密钥"
                # 重新获取密钥数量
                existing_keys_count=$(gcloud iam service-accounts keys list \
                    --iam-account="$sa_email" \
                    --project="$project_id" \
                    --format='value(name)' 2>/dev/null | wc -l)
                log "INFO" "服务账号当前有 ${existing_keys_count} 个密钥"
            else
                log "ERROR" "清理旧密钥失败，请手动删除"
                return 1
            fi
        else
            return 1
        fi
    fi
    
    log "INFO" "服务账号当前有 ${existing_keys_count} 个密钥，可以创建新密钥"
    
    # 生成密钥
    log "INFO" "生成服务账号密钥..."
    local key_file="${KEY_DIR}/${project_id}-${SERVICE_ACCOUNT_NAME}-$(date +%Y%m%d-%H%M%S).json"
    
    # 执行命令，保留错误输出以便调试
    log "INFO" "执行 gcloud 命令..."
    local gcloud_exit_code=0
    local gcloud_error_output=""
    
    # 显式地将gcloud命令的标准输出（即密钥内容）重定向到文件中，以确保行为一致性
    # 用户反馈表明，在某些环境下gcloud会将密钥打印到控制台而不是直接写入文件
    gcloud_error_output=$(gcloud iam service-accounts keys create - \
        --iam-account="$sa_email" \
        --project="$project_id" \
        --quiet 2>&1 > "$key_file")
    gcloud_exit_code=$?
    
    log "INFO" "gcloud 命令退出码: ${gcloud_exit_code}"
    
    # 如果有错误输出，记录详细信息
    if [ $gcloud_exit_code -ne 0 ] && [ -n "$gcloud_error_output" ]; then
        log "ERROR" "gcloud 命令错误详情: ${gcloud_error_output}"
        
        # 根据错误类型提供解决方案
        if [[ "$gcloud_error_output" == *"FAILED_PRECONDITION"* ]]; then
            log "ERROR" "前置条件检查失败，可能的原因:"
            log "ERROR" "1. 服务账号密钥数量已达到上限(10个)"
            log "ERROR" "2. 当前用户缺少创建密钥的权限"
            log "ERROR" "3. 项目状态异常或API未正确启用"
            log "INFO" "建议执行以下命令检查权限:"
            log "INFO" "gcloud projects get-iam-policy ${project_id} --flatten='bindings[].members' --format='table(bindings.role)' --filter='bindings.members:$(gcloud config get-value account)'"
        elif [[ "$gcloud_error_output" == *"PERMISSION_DENIED"* ]]; then
            log "ERROR" "权限被拒绝，请检查是否有足够的IAM权限"
        fi
    fi

    # 检查文件是否真的被创建
    if [ $gcloud_exit_code -eq 0 ] && [ -f "$key_file" ] && [ -s "$key_file" ]; then
        chmod 600 "$key_file"
        log "SUCCESS" "密钥已保存: ${key_file}"
        echo "$key_file"
        return 0
    else
        log "ERROR" "生成密钥失败 (文件未在预期位置找到，gcloud退出码: ${gcloud_exit_code})"
        rm -f "$key_file" 2>/dev/null
        return 1
    fi
}

# 创建Vertex项目
vertex_create_projects() {
    log "INFO" "====== 创建新项目并配置 Vertex AI ======"
    
    # 获取当前结算账户的项目数
    log "INFO" "检查结算账户 ${BILLING_ACCOUNT} 的项目数..."
    local existing_projects
    existing_projects=$(gcloud projects list --filter="billingAccountName:billingAccounts/${BILLING_ACCOUNT}" --format='value(projectId)' 2>/dev/null | wc -l)
    
    log "INFO" "当前已有 ${existing_projects} 个项目"
    
    local max_new=$((MAX_PROJECTS_PER_ACCOUNT - existing_projects))
    if [ "$max_new" -le 0 ]; then
        log "WARN" "结算账户已达到最大项目数限制 (${MAX_PROJECTS_PER_ACCOUNT})"
        return 1
    fi
    
    # 询问创建数量
    log "INFO" "最多可创建 ${max_new} 个新项目"
    local num_projects
    read -r -p "请输入要创建的项目数量 [1-${max_new}]: " num_projects
    
    if ! [[ "$num_projects" =~ ^[0-9]+$ ]] || [ "$num_projects" -lt 1 ] || [ "$num_projects" -gt "$max_new" ]; then
        log "ERROR" "无效的项目数量"
        return 1
    fi
    
    # 询问项目前缀
    local project_prefix
    read -r -p "请输入项目前缀 (默认: vertex): " project_prefix
    project_prefix=${project_prefix:-vertex}
    
    # 确认操作
    echo -e "\n${YELLOW}即将创建 ${num_projects} 个项目${NC}"
    echo "项目前缀: ${project_prefix}"
    echo "结算账户: ${BILLING_ACCOUNT}"
    echo
    
    if ! ask_yes_no "确认继续？" "N"; then
        log "INFO" "操作已取消"
        return 1
    fi
    
    # 创建项目
    log "INFO" "开始创建项目..."
    local success=0
    local failed=0
    
    local i=1
    while [ $i -le $num_projects ]; do
        local project_id
        project_id=$(new_project_id "$project_prefix")
        
        log "INFO" "[${i}/${num_projects}] 创建项目: ${project_id}"
        
        # 创建项目
        if ! retry gcloud projects create "$project_id" --quiet; then
            log "ERROR" "创建项目失败: ${project_id}"
            failed=$((failed + 1)) || true
            show_progress "$i" "$num_projects"
            continue
        fi
        
        # 关联结算账户
        log "INFO" "关联结算账户..."
        if ! retry gcloud billing projects link "$project_id" --billing-account="$BILLING_ACCOUNT" --quiet; then
            log "ERROR" "关联结算账户失败: ${project_id}"
            gcloud projects delete "$project_id" --quiet 2>/dev/null
            failed=$((failed + 1)) || true
            show_progress "$i" "$num_projects"
            continue
        fi
        
        # 启用API
        log "INFO" "启用必要的API..."
        if ! enable_services "$project_id"; then
            log "ERROR" "启用API失败: ${project_id}"
            failed=$((failed + 1)) || true
            show_progress "$i" "$num_projects"
            continue
        fi
        
        # 配置服务账号
        log "INFO" "配置服务账号..."
        if vertex_setup_service_account "$project_id"; then
            log "SUCCESS" "成功配置项目: ${project_id}"
            success=$((success + 1)) || true
        else
            log "ERROR" "配置服务账号失败: ${project_id}"
            failed=$((failed + 1)) || true
        fi
        
        show_progress "$i" "$num_projects"
        
        # 避免过快请求
        sleep 2
        
        # 递增计数器
        i=$((i + 1)) || true
    done
    
    # 显示结果
    echo -e "\n${GREEN}操作完成！${NC}"
    echo "成功: ${success}, 失败: ${failed}"
    echo "服务账号密钥已保存在: ${KEY_DIR}"
}

# 配置现有项目的Vertex AI
vertex_configure_existing() {
    log "INFO" "====== 在现有项目上配置 Vertex AI ======"
    
    # 获取项目列表
    log "INFO" "获取项目列表..."
    local projects
    # 先获取所有活跃项目
    local all_projects
    all_projects=$(gcloud projects list --format='value(projectId)' --filter="lifecycleState=ACTIVE" 2>/dev/null || echo "")
    
    # 筛选出与当前结算账户关联的项目
    local projects=""
    while IFS= read -r project_id; do
        if [ -n "$project_id" ]; then
            local billing_info
            billing_info=$(gcloud billing projects describe "$project_id" --format='value(billingAccountName)' 2>/dev/null || echo "")
            
            if [ -n "$billing_info" ] && [[ "$billing_info" == *"${BILLING_ACCOUNT}"* ]]; then
                projects="${projects}${projects:+$'\n'}${project_id}"
            fi
        fi
    done <<< "$all_projects"
    
    # 如果没有找到与结算账户关联的项目，提示用户
    if [ -z "$projects" ]; then
        log "WARN" "未找到与当前结算账户关联的项目"
        echo -e "\n${YELLOW}请选择操作:${NC}"
        echo "1. 显示所有项目（包括未关联当前结算账户的项目）"
        echo "2. 返回上级菜单"
        
        local list_choice
        read -r -p "请选择 [1-2]: " list_choice
        
        case "$list_choice" in
            1)
                log "INFO" "显示所有活跃项目"
                projects=$(gcloud projects list --format='value(projectId)' --filter='lifecycleState:ACTIVE' 2>/dev/null || echo "")
                ;;
            *)
                log "INFO" "返回上级菜单"
                return 0
                ;;
        esac
    else
        log "INFO" "找到与结算账户 ${BILLING_ACCOUNT} 关联的项目"
    fi
    
    if [ -z "$projects" ]; then
        log "ERROR" "未找到任何活跃项目"
        return 1
    fi
    
    # 转换为数组
    local project_array=()
    while IFS= read -r line; do
        if [ -n "$line" ]; then
            project_array+=("$line")
        fi
    done <<< "$projects"
    
    local total=${#project_array[@]}
    
    # 检查是否找到项目
    if [ "$total" -eq 0 ]; then
        log "WARN" "未找到与当前结算账户关联的项目"
        echo -e "\n${YELLOW}请选择操作:${NC}"
        echo "1. 显示所有项目（包括未关联当前结算账户的项目）"
        echo "2. 返回上级菜单"
        
        local list_choice
        read -r -p "请选择 [1-2]: " list_choice
        
        case "$list_choice" in
            1)
                log "INFO" "显示所有活跃项目"
                # 使用先前获取的所有项目
                while IFS= read -r line; do
                    if [ -n "$line" ]; then
                        project_array+=("$line")
                    fi
                done <<< "$all_projects"
                total=${#project_array[@]}
                ;;
            *)
                log "INFO" "返回上级菜单"
                return 0
                ;;
        esac
    else
        log "INFO" "找到 ${total} 个与当前结算账户关联的项目"
    fi
    
    # 显示项目列表
    echo -e "\n项目列表:"
    for ((i=0; i<total && i<20; i++)); do
        local billing_info
        billing_info=$(gcloud billing projects describe "${project_array[i]}" --format='value(billingAccountName)' 2>/dev/null || echo "")
        
        local status=""
        if [ -n "$billing_info" ] && [[ "$billing_info" == *"${BILLING_ACCOUNT}"* ]]; then
            status="(已关联当前结算账户)"
        elif [ -n "$billing_info" ]; then
            status="(关联了其他结算账户)"
        else
            status="(未关联结算)"
        fi
        
        echo "$((i+1)). ${project_array[i]} ${status}"
    done
    
    if [ "$total" -gt 20 ]; then
        echo "... 还有 $((total-20)) 个项目"
    fi
    
    # 选择项目
    local selected_projects=()
    read -r -p "请输入项目编号（多个用空格分隔）: " -a numbers
    
    for num in "${numbers[@]}"; do
        if [[ "$num" =~ ^[0-9]+$ ]] && [ "$num" -ge 1 ] && [ "$num" -le "$total" ]; then
            selected_projects+=("${project_array[$((num-1))]}")
        fi
    done
    
    if [ ${#selected_projects[@]} -eq 0 ]; then
        log "ERROR" "未选择任何项目"
        return 1
    fi
    
    # 确认操作
    echo -e "\n${YELLOW}将为 ${#selected_projects[@]} 个项目配置 Vertex AI${NC}"
    if ! ask_yes_no "确认继续？" "N"; then
        log "INFO" "操作已取消"
        return 1
    fi
    
    # 处理选定的项目
    local success=0
    local failed=0
    local current=0
    
    for project_id in "${selected_projects[@]}"; do
        current=$((current + 1)) || true
        log "INFO" "[${current}/${#selected_projects[@]}] 处理项目: ${project_id}"
        
        # 检查结算账户
        local billing_info
        billing_info=$(gcloud billing projects describe "$project_id" --format='value(billingAccountName)' 2>/dev/null || echo "")
        
        if [ -z "$billing_info" ]; then
            log "WARN" "项目未关联结算账户，尝试关联..."
            if ! retry gcloud billing projects link "$project_id" --billing-account="$BILLING_ACCOUNT" --quiet; then
                log "ERROR" "关联结算账户失败: ${project_id}"
                failed=$((failed + 1)) || true
                show_progress "$current" "${#selected_projects[@]}"
                continue
            fi
        fi
        
        # 启用API
        log "INFO" "启用必要的API..."
        if ! enable_services "$project_id"; then
            log "ERROR" "启用API失败: ${project_id}"
            failed=$((failed + 1)) || true
            show_progress "$current" "${#selected_projects[@]}"
            continue
        fi
        
        # 配置服务账号
        log "INFO" "配置服务账号..."
        if vertex_setup_service_account "$project_id"; then
            log "SUCCESS" "成功配置项目: ${project_id}"
            success=$((success + 1)) || true
        else
            log "ERROR" "配置服务账号失败: ${project_id}"
            failed=$((failed + 1)) || true
        fi
        
        show_progress "$current" "${#selected_projects[@]}"
    done
    
    # 显示结果
    echo -e "\n${GREEN}操作完成！${NC}"
    echo "成功: ${success}, 失败: ${failed}"
    echo "服务账号密钥已保存在: ${KEY_DIR}"
}

# 管理Vertex服务账号密钥
vertex_manage_keys() {
    log "INFO" "====== 管理服务账号密钥 ======"
    
    echo "请选择操作:"
    echo "1. 列出所有服务账号密钥"
    echo "2. 生成新密钥"
    echo "3. 删除旧密钥"
    echo "0. 返回"
    echo
    
    local choice
    read -r -p "请选择 [0-3]: " choice
    
    case "$choice" in
        1) vertex_list_keys ;;
        2) vertex_generate_keys ;;
        3) vertex_delete_keys ;;
        0) return 0 ;;
        *) log "ERROR" "无效选项"; return 1 ;;
    esac
}

# 列出Vertex密钥
vertex_list_keys() {
    log "INFO" "扫描密钥目录: ${KEY_DIR}"
    
    if [ ! -d "$KEY_DIR" ]; then
        log "ERROR" "密钥目录不存在"
        return 1
    fi
    
    local key_files=()
    while IFS= read -r -d '' file; do
        key_files+=("$file")
    done < <(find "$KEY_DIR" -name "*.json" -type f -print0 2>/dev/null)
    
    if [ ${#key_files[@]} -eq 0 ]; then
        log "INFO" "未找到任何密钥文件"
        return 0
    fi
    
    echo -e "\n找到 ${#key_files[@]} 个密钥文件:"
    for ((i=0; i<${#key_files[@]}; i++)); do
        local filename
        filename=$(basename "${key_files[i]}")
        local size
        size=$(stat -f%z "${key_files[i]}" 2>/dev/null || stat -c%s "${key_files[i]}" 2>/dev/null || echo "unknown")
        echo "$((i+1)). ${filename} (${size} bytes)"
    done
}

# 生成新的Vertex密钥
vertex_generate_keys() {
    log "INFO" "====== 生成新的服务账号密钥 ======"
    
    # 获取项目列表
    log "INFO" "获取项目列表..."
    local projects
    projects=$(gcloud projects list --format='value(projectId)' --filter='lifecycleState:ACTIVE' 2>/dev/null || echo "")
    
    if [ -z "$projects" ]; then
        log "ERROR" "未找到任何活跃项目"
        return 1
    fi
    
    # 转换为数组
    local project_array=()
    while IFS= read -r line; do
        if [ -n "$line" ]; then
            project_array+=("$line")
        fi
    done <<< "$projects"
    
    local total=${#project_array[@]}
    log "INFO" "找到 ${total} 个项目"
    
    # 显示项目列表
    echo -e "\n项目列表:"
    for ((i=0; i<total && i<20; i++)); do
        echo "$((i+1)). ${project_array[i]}"
    done
    
    if [ "$total" -gt 20 ]; then
        echo "... 还有 $((total-20)) 个项目"
    fi
    
    # 选择项目
    local selected_projects=()
    read -r -p "请输入项目编号（多个用空格分隔）: " -a numbers
    
    for num in "${numbers[@]}"; do
        if [[ "$num" =~ ^[0-9]+$ ]] && [ "$num" -ge 1 ] && [ "$num" -le "$total" ]; then
            selected_projects+=("${project_array[$((num-1))]}")
        fi
    done
    
    if [ ${#selected_projects[@]} -eq 0 ]; then
        log "ERROR" "未选择任何项目"
        return 1
    fi
    
    # 确认操作
    echo -e "\n${YELLOW}将为 ${#selected_projects[@]} 个项目生成新密钥${NC}"
    if ! ask_yes_no "确认继续？" "N"; then
        log "INFO" "操作已取消"
        return 1
    fi
    
    # 处理选定的项目
    local success=0
    local failed=0
    local current=0
    
    for project_id in "${selected_projects[@]}"; do
        current=$((current + 1)) || true
        log "INFO" "[${current}/${#selected_projects[@]}] 处理项目: ${project_id}"
        
        if vertex_setup_service_account "$project_id"; then
            log "SUCCESS" "成功配置项目: ${project_id}"
            success=$((success + 1)) || true
        else
            log "ERROR" "配置服务账号失败: ${project_id}"
            failed=$((failed + 1)) || true
        fi
        
        show_progress "$current" "${#selected_projects[@]}"
    done
    
    # 显示结果
    echo -e "\n${GREEN}操作完成！${NC}"
    echo "成功: ${success}, 失败: ${failed}"
    echo "密钥已保存在: ${KEY_DIR}"
}

# 删除旧的Vertex密钥
vertex_delete_keys() {
    log "INFO" "====== 删除旧的服务账号密钥 ======"
    
    if [ ! -d "$KEY_DIR" ]; then
        log "ERROR" "密钥目录不存在"
        return 1
    fi
    
    local key_files=()
    while IFS= read -r -d '' file; do
        key_files+=("$file")
    done < <(find "$KEY_DIR" -name "*.json" -type f -print0 2>/dev/null)
    
    if [ ${#key_files[@]} -eq 0 ]; then
        log "INFO" "未找到任何密钥文件"
        return 0
    fi
    
    echo -e "\n找到 ${#key_files[@]} 个密钥文件:"
    for ((i=0; i<${#key_files[@]}; i++)); do
        local filename
        filename=$(basename "${key_files[i]}")
        echo "$((i+1)). ${filename}"
    done
    
    # 选择要删除的文件
    read -r -p "请输入要删除的文件编号（多个用空格分隔）: " -a numbers
    
    local selected_files=()
    for num in "${numbers[@]}"; do
        if [[ "$num" =~ ^[0-9]+$ ]] && [ "$num" -ge 1 ] && [ "$num" -le "${#key_files[@]}" ]; then
            selected_files+=("${key_files[$((num-1))]}")
        fi
    done
    
    if [ ${#selected_files[@]} -eq 0 ]; then
        log "ERROR" "未选择任何文件"
        return 1
    fi
    
    # 确认删除
    echo -e "\n${RED}${BOLD}警告: 即将删除 ${#selected_files[@]} 个密钥文件！${NC}"
    echo -e "${RED}此操作不可撤销！${NC}"
    echo
    echo "将删除的文件:"
    for file in "${selected_files[@]}"; do
        echo "  - $(basename "$file")"
    done
    echo
    
    read -r -p "请输入 'DELETE' 确认删除: " confirm
    
    if [ "$confirm" != "DELETE" ]; then
        log "INFO" "删除操作已取消"
        return 1
    fi
    
    # 执行删除
    local success=0
    local failed=0
    
    for file in "${selected_files[@]}"; do
        if rm -f "$file" 2>/dev/null; then
            log "SUCCESS" "成功删除: $(basename "$file")"
            success=$((success + 1)) || true
        else
            log "ERROR" "删除失败: $(basename "$file")"
            failed=$((failed + 1)) || true
        fi
    done
    
    # 显示结果
    echo -e "\n${GREEN}操作完成！${NC}"
    echo "成功删除: ${success}"
    echo "删除失败: ${failed}"
}

# ===== 主菜单 =====

# 显示主菜单
show_menu() {
    echo -e "\n${CYAN}${BOLD}======================================================"
    echo -e "     Vertex AI 密钥管理工具 v${VERSION}"
    echo -e "     更新日期: ${LAST_UPDATED}"
    echo -e "======================================================${NC}\n"
    
    # 显示当前账号信息
    local current_account
    current_account=$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | head -n 1)
    local current_project
    current_project=$(gcloud config get-value project 2>/dev/null || echo "未设置")
    
    echo "当前账号: ${current_account:-未登录}"
    echo "当前项目: ${current_project}"
    echo
    
    # 风险提示
    echo -e "${RED}${BOLD}⚠️  风险提示 ⚠️${NC}"
    echo -e "${YELLOW}• Vertex AI 会产生实际费用${NC}"
    echo
    
    # 直接进入 Vertex AI 管理
    vertex_main "$@"
}



# ===== 主程序入口 =====

main() {
    local num_to_process=1 # 默认处理1个

    # 解析命令行参数
    while [ $# -gt 0 ]; do
        case "$1" in
            -n|--count)
                if [[ "$2" =~ ^[0-9]+$ ]] && [ "$2" -gt 0 ]; then
                    num_to_process="$2"
                    shift 2
                else
                    log "ERROR" "参数 '$1' 需要一个正整数值"
                    exit 1
                fi
                ;;
            --s3-endpoint)
                S3_ENDPOINT="$2"
                shift 2
                ;;
            --s3-access-key)
                S3_ACCESS_KEY="$2"
                shift 2
                ;;
            --s3-secret-key)
                S3_SECRET_KEY="$2"
                shift 2
                ;;
            --s3-bucket)
                S3_BUCKET="$2"
                shift 2
                ;;
            --s3-directory)
                S3_DIRECTORY="$2"
                shift 2
                ;;
            -h|--help)
                echo "用法: $0 [-n COUNT] [S3选项]"
                echo "  -n, --count         要生成并打印JSON密钥的项目数量 (默认为 1)"
                echo "  --s3-endpoint       S3端点地址"
                echo "  --s3-access-key     S3访问密钥"
                echo "  --s3-secret-key     S3秘密密钥"
                echo "  --s3-bucket         S3存储桶名称"
                echo "  --s3-directory      S3目录名 (默认为当前日期，如 20240720)"
                echo "  -h, --help          显示此帮助信息"
                echo ""
                echo "S3示例:"
                echo "  $0 -n 3 --s3-endpoint https://s3.amazonaws.com --s3-access-key AKIAXXXXXXXX \\"
                echo "    --s3-secret-key your-secret-key --s3-bucket my-bucket --s3-directory keys-backup"
                exit 0
                ;;
            *)
                log "ERROR" "未知选项: $1. 使用 -h 或 --help 查看帮助."
                exit 1
                ;;
        esac
    done
    
    # 生成S3目录（如果S3已配置）
    if check_s3_config; then
        generate_s3_directory
        log "INFO" "S3配置已检测到，目标位置: s3://${S3_BUCKET}/${S3_DIRECTORY}/"
        log "INFO" "S3上传功能将在检测到AWS CLI后启用"
    fi

    # 显示欢迎信息
    echo -e "${CYAN}${BOLD}"
    echo "╔═══════════════════════════════════════════════════════╗"
    echo "║          GCP API 密钥管理工具 v${VERSION}                  ║"
    echo "║                                                       ║"
    echo "║                  Vertex AI 专用版                     ║"
    echo "╚═══════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    # 运行主程序逻辑，传递参数
    show_menu "$num_to_process"
}

# 运行主程序
main "$@"
