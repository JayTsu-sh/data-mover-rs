// Windows ACL 操作需要大量 FFI 调用（SID/DACL 指针、LocalFree 等）
#![allow(unsafe_code)]

// 标准库
use std::ffi::c_void;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::{fmt, ptr};

// 外部crate
use windows::core::PWSTR;
use windows_sys::Win32::Foundation::{ERROR_SUCCESS, GetLastError, LocalFree, MAX_PATH};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, GetNamedSecurityInfoW, SE_FILE_OBJECT, SetNamedSecurityInfoW,
};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACCESS_DENIED_ACE, ACE_HEADER, ACL, ACL_REVISION, ACL_SIZE_INFORMATION,
    AclSizeInformation, AddAce, DACL_SECURITY_INFORMATION, GROUP_SECURITY_INFORMATION, GetAce,
    GetAclInformation, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
    GetSecurityDescriptorLength, INHERITED_ACE, InitializeAcl, IsValidSecurityDescriptor,
    IsValidSid, LookupAccountSidW, OWNER_SECURITY_INFORMATION, PSID, SE_DACL_PROTECTED,
    SECURITY_DESCRIPTOR_CONTROL, SID, SID_NAME_USE, SetSecurityDescriptorControl,
};
use windows_sys::Win32::System::Memory::{LMEM_FIXED, LocalAlloc};
use windows_sys::Win32::System::SystemServices::{ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE};

// 内部模块
use crate::error::{Result, StorageError};

// ACE类型枚举
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AceType {
    Allow,
    Deny,
    Other(u8),
}

// 权限枚举 - 包含13种基本权限
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    // 基本权限（13种）
    ListFolderReadData = 0x00000001,      // 1. 列出文件夹/读数据
    CreateFileWriteData = 0x00000002,     // 2. 创建文件/写数据
    CreateFolderAppend = 0x00000004,      // 3. 创建文件夹/附加
    ReadExtendedAttributes = 0x00000008,  // 4. 读取扩展属性
    WriteExtendedAttributes = 0x00000010, // 5. 写入扩展属性
    TraverseExecuteFile = 0x00000020,     // 6. 遍历/执行文件
    ReadAttributes = 0x00000080,          // 7. 读取属性
    WriteAttributes = 0x00000100,         // 8. 写入属性
    Delete = 0x00010000,                  // 9. 删除
    ReadPermissions = 0x00020000,         // 10. 读取权限
    ChangePermissions = 0x00040000,       // 11. 更改权限
    TakeOwnership = 0x00080000,           // 12. 取得所有权

    // 组合权限（常用）
    Read = 0x00120089,           // 读取
    Write = 0x00120116,          // 写入
    ReadAndExecute = 0x001200A9, // 读取和执行
    Modify = 0x001301BF,         // 修改
    FullControl = 0x001F01FF,    // 完全控制
}

// 权限条目结构体
#[derive(Debug, Clone)]
pub struct PermissionEntry {
    pub ace_type: AceType,
    pub permissions: Vec<Permission>,
}

impl fmt::Display for PermissionEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: [", self.ace_type)?;
        for (i, interface) in self.permissions.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", interface)?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

// 权限映射表
pub struct PermissionMapping {
    // 不再需要静态映射，使用动态解析
}

impl PermissionMapping {
    pub fn new() -> Self {
        PermissionMapping {}
    }

    // 动态解析权限掩码
    fn parse_mask(&self, mask: u32) -> Vec<Permission> {
        // 检查完整权限组合（优先级最高）
        const COMPOSITE_PERMISSIONS: [Permission; 5] = [
            Permission::FullControl,
            Permission::Modify,
            Permission::ReadAndExecute,
            Permission::Write,
            Permission::Read,
        ];

        for &perm in &COMPOSITE_PERMISSIONS {
            if mask == perm as u32 {
                return vec![perm];
            }
        }

        // 检查读取+写入组合
        if mask == 0x0012019F {
            return vec![Permission::Read, Permission::Write];
        }

        // 遍历所有单个权限变体（数组元素唯一，无需去重）
        const SINGLE_PERMISSIONS: [Permission; 12] = [
            Permission::ListFolderReadData,
            Permission::CreateFileWriteData,
            Permission::CreateFolderAppend,
            Permission::ReadExtendedAttributes,
            Permission::WriteExtendedAttributes,
            Permission::TraverseExecuteFile,
            Permission::ReadAttributes,
            Permission::WriteAttributes,
            Permission::Delete,
            Permission::ReadPermissions,
            Permission::ChangePermissions,
            Permission::TakeOwnership,
        ];

        let mut permissions = Vec::new();
        for &perm in &SINGLE_PERMISSIONS {
            if (mask & perm as u32) == perm as u32 {
                permissions.push(perm);
            }
        }

        // 如果没有匹配到单个权限，尝试匹配组合权限
        if permissions.is_empty() {
            for &perm in &COMPOSITE_PERMISSIONS {
                if (mask & perm as u32) == perm as u32 {
                    return vec![perm];
                }
            }
        }

        permissions
    }

    // 根据掩码获取权限列表
    pub fn get_permissions(&self, mask: u32) -> Vec<Permission> {
        self.parse_mask(mask)
    }

    // 兼容旧方法签名
    pub fn get_permission(&self, mask: u32, _ace_flags: u32) -> Option<PermissionEntry> {
        // 动态解析权限
        let permissions = self.parse_mask(mask);
        if permissions.is_empty() {
            None
        } else {
            // 注意：这里返回的ace_type是默认值，实际使用时应该从ACE头获取
            // 这个方法主要用于向后兼容
            Some(PermissionEntry {
                ace_type: AceType::Allow, // 默认值
                permissions,
            })
        }
    }
}

// 为AceType实现Display trait
impl fmt::Display for AceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AceType::Allow => write!(f, "允许"),
            AceType::Deny => write!(f, "拒绝"),
            AceType::Other(t) => write!(f, "其他类型 ({})", t),
        }
    }
}

// 为PermissionInterface实现Display trait
impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // 基本权限
            Permission::FullControl => write!(f, "完全控制"),
            Permission::TraverseExecuteFile => write!(f, "遍历/执行文件"),
            Permission::ListFolderReadData => write!(f, "列出文件夹/读数据"),
            Permission::ReadAttributes => write!(f, "读取属性"),
            Permission::ReadExtendedAttributes => write!(f, "读取扩展属性"),
            Permission::CreateFileWriteData => write!(f, "创建文件/写数据"),
            Permission::CreateFolderAppend => write!(f, "创建文件夹/附加"),
            Permission::WriteAttributes => write!(f, "写入属性"),
            Permission::WriteExtendedAttributes => write!(f, "写入扩展属性"),
            Permission::Delete => write!(f, "删除"),
            Permission::ReadPermissions => write!(f, "读取权限"),
            Permission::ChangePermissions => write!(f, "更改权限"),
            Permission::TakeOwnership => write!(f, "取得所有权"),

            // 组合权限
            Permission::Read => write!(f, "读取"),
            Permission::Write => write!(f, "写入"),
            Permission::ReadAndExecute => write!(f, "读取和执行"),
            Permission::Modify => write!(f, "修改"),
        }
    }
}

// 为PermissionMapping实现Default trait
impl Default for PermissionMapping {
    fn default() -> Self {
        Self::new()
    }
}

// ACE信息结构体
#[derive(Debug, Clone)]
pub struct AceInfo {
    pub trustee: String,
    pub trustee_type: String,
    pub access_mode: String,
    pub inherited: bool,
    pub ace_type: u8,
    pub mask: u32,
    pub ace_flags: u8, // ACE标志位
}

#[derive(Debug, Clone)]
pub struct SecurityInfo {
    pub path: PathBuf,
    pub security_descriptor: Vec<u8>,
    pub owner: String,
    pub primary_group: String,
    pub has_dacl: bool,
    pub aces: Vec<AceInfo>,
    pub inheritance_enabled: bool, // true-继承启用状态, false-继承禁用状态
}

impl SecurityInfo {
    pub fn get_aces(&self, inherited: bool) -> Vec<&AceInfo> {
        if inherited {
            self.aces.iter().collect()
        } else {
            self.aces.iter().filter(|ace| !ace.inherited).collect()
        }
    }
}

// Implement Display trait for AceInfo
impl fmt::Display for AceInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Trustee: {}", self.trustee)?;
        write!(f, ", Type: {}", self.trustee_type)?;
        write!(f, ", Access Mode: {}", self.access_mode)?;
        write!(
            f,
            ", Inherited: {}",
            if self.inherited { "Yes" } else { "No" }
        )?;
        write!(f, ", ACE Type: 0x{:02X}", self.ace_type)?;
        write!(f, ", Mask: 0x{:08X}", self.mask)?;
        write!(
            f,
            ", Flags: 0x{:02X} ({})",
            self.ace_flags,
            parse_ace_flags(self.ace_flags)
        )
    }
}

// Implement Display trait for SecurityInfo
impl fmt::Display for SecurityInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Path: {}", self.path.display())?;
        write!(f, ", Owner: {}", self.owner)?;
        write!(f, ", Primary Group: {}", self.primary_group)?;
        write!(
            f,
            ", Has DACL: {}",
            if self.has_dacl { "Yes" } else { "No" }
        )?;
        write!(
            f,
            ", Inheritance Enabled: {}",
            if self.inheritance_enabled {
                "Yes"
            } else {
                "No"
            }
        )?;
        write!(f, ", ACE Count: {}", self.aces.len())
    }
}

fn path_to_wide(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

/// 读取目录自身的ACE（不包括继承的ACE）
pub fn get_security_info(path: &Path) -> Result<SecurityInfo> {
    let path_wide = path_to_wide(path);

    #[allow(unsafe_code)]
    unsafe {
        let (sd, owner_sid, group_sid, dacl) = get_security_descriptor(path_wide.as_ptr())?;

        // 保存安全描述符的副本
        let sd_size = GetSecurityDescriptorLength(sd);
        let mut security_descriptor = Vec::with_capacity(sd_size as usize);
        security_descriptor.set_len(sd_size as usize);
        std::ptr::copy_nonoverlapping(
            sd as *const u8,
            security_descriptor.as_mut_ptr(),
            sd_size as usize,
        );

        // 获取所有者信息
        let owner = if !owner_sid.is_null() && IsValidSid(owner_sid) != 0 {
            lookup_sid_name(owner_sid).unwrap_or_else(|_| "unresolved owner".to_string())
        } else {
            "no owner".to_string()
        };

        // 获取主组信息
        let primary_group = if !group_sid.is_null() && IsValidSid(group_sid) != 0 {
            lookup_sid_name(group_sid).unwrap_or_else(|_| "unresolved group".to_string())
        } else {
            "no group".to_string()
        };

        // 获取继承状态
        let inheritance_enabled = get_inheritance_enabled(sd)?;

        // 检查DACL是否存在
        if dacl.is_null() {
            LocalFree(sd as *mut _);
            return Ok(SecurityInfo {
                path: path.to_path_buf(),
                owner,
                primary_group,
                has_dacl: false,
                aces: Vec::new(),
                security_descriptor,
                inheritance_enabled,
            }); // 没有DACL，返回空列表
        }

        // 获取DACL信息
        let mut dacl_present: i32 = 0;
        let mut dacl_defaulted: i32 = 0;
        let mut temp_dacl = std::ptr::null_mut();

        let has_dacl =
            GetSecurityDescriptorDacl(sd, &mut dacl_present, &mut temp_dacl, &mut dacl_defaulted)
                != 0;

        // 解析DACL中的ACE
        let aces = if has_dacl && dacl_present != 0 && !temp_dacl.is_null() {
            parse_dacl_entries(temp_dacl).unwrap_or_else(|e| {
                eprintln!("Failed to parse DACL: {}", e);
                Vec::new()
            })
        } else {
            Vec::new()
        };

        // 释放资源
        LocalFree(sd as *mut _);

        Ok(SecurityInfo {
            path: path.to_path_buf(),
            security_descriptor,
            owner,
            primary_group,
            has_dacl: true,
            aces,
            inheritance_enabled,
        })
    }
}

fn parse_dacl_entries(dacl: *mut ACL) -> Result<Vec<AceInfo>> {
    let mut aces = Vec::new();

    #[allow(unsafe_code)]
    unsafe {
        let mut acl_size_info: ACL_SIZE_INFORMATION = std::mem::zeroed();

        if GetAclInformation(
            dacl,
            &mut acl_size_info as *mut _ as *mut _,
            std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        ) == 0
        {
            return Err(StorageError::WinAceError(
                "Failed to get DACL information".to_string(),
            ));
        }

        let ace_count = acl_size_info.AceCount;

        // 遍历所有ACE
        for i in 0..ace_count {
            let mut ace_ptr: *mut c_void = std::ptr::null_mut();
            if GetAce(dacl, i, &mut ace_ptr) != 0 {
                let ace = ace_ptr as *mut ACE_HEADER;
                if let Some(ace_info) = parse_ace(ace) {
                    aces.push(ace_info);
                }
            }
        }
    }

    Ok(aces)
}

fn parse_ace(ace_header: *mut ACE_HEADER) -> Option<AceInfo> {
    #[allow(unsafe_code)]
    unsafe {
        let ace_type = (*ace_header).AceType;
        let ace_flags = (*ace_header).AceFlags;

        // 定义ACE类型常量作为u8
        const ALLOWED_TYPE: u8 = ACCESS_ALLOWED_ACE_TYPE as u8;
        const DENIED_TYPE: u8 = ACCESS_DENIED_ACE_TYPE as u8;

        let (mask, access_mode, sid_ptr) = match ace_type {
            ALLOWED_TYPE => {
                let ace = ace_header as *mut ACCESS_ALLOWED_ACE;
                (
                    (*ace).Mask,
                    "Allow",
                    &(*ace).SidStart as *const _ as *mut SID,
                )
            }
            DENIED_TYPE => {
                let ace = ace_header as *mut ACCESS_DENIED_ACE;
                (
                    (*ace).Mask,
                    "Deny",
                    &(*ace).SidStart as *const _ as *mut SID,
                )
            }
            // 对于其他类型的ACE，我们仍然尝试解析基本信息
            _ => (0u32, "other", (ace_header as *mut u8).offset(8) as *mut SID),
        };

        parse_ace_common(sid_ptr, access_mode, ace_flags, mask, ace_type)
    }
}

// 解析ACE标志，返回人类可读的继承描述
fn parse_ace_flags(ace_flags: u8) -> String {
    match ace_flags {
        0x00 => "直接ACE（无标志）".to_string(),
        0x01 => "对象继承（适用于文件）".to_string(),
        0x02 => "容器继承（适用于目录）".to_string(),
        0x03 => "对象+容器继承".to_string(),
        0x10 => "继承的ACE（通用）".to_string(),
        0x11 => "继承的对象继承ACE".to_string(),
        0x12 => "继承的容器继承ACE".to_string(),
        0x13 => "继承的对象+容器继承ACE".to_string(),
        _ => format!("未知ACE标志: 0x{:02X}", ace_flags),
    }
}

fn parse_ace_common(
    sid: *mut SID,
    access_mode: &str,
    ace_flags: u8,
    mask: u32,
    ace_type: u8,
) -> Option<AceInfo> {
    let trustee = lookup_sid_name(sid as PSID).unwrap_or_else(|_| "unknown trustee".to_string());

    let inherited = (ace_flags & INHERITED_ACE as u8) != 0;

    #[allow(unsafe_code)]
    let trustee_type = unsafe {
        if IsValidSid(sid as PSID) != 0 {
            // 这里可以进一步判断SID类型（用户、组等）
            "user/group".to_string()
        } else {
            "unknown".to_string()
        }
    };

    Some(AceInfo {
        trustee,
        trustee_type,
        access_mode: access_mode.to_string(),
        inherited,
        mask,
        ace_flags,
        ace_type,
    })
}

fn lookup_sid_name(sid: PSID) -> Result<String> {
    #[allow(unsafe_code)]
    unsafe {
        let mut name_buffer = [0u16; MAX_PATH as usize];
        let mut domain_buffer = [0u16; MAX_PATH as usize];
        let mut name_size = name_buffer.len() as u32;
        let mut domain_size = domain_buffer.len() as u32;
        let mut sid_name_use = SID_NAME_USE::default();

        let result = LookupAccountSidW(
            std::ptr::null(), // 本地计算机
            sid,
            name_buffer.as_mut_ptr(),
            &mut name_size,
            domain_buffer.as_mut_ptr(),
            &mut domain_size,
            &mut sid_name_use,
        );

        if result != 0 {
            let name = String::from_utf16_lossy(&name_buffer[..name_size as usize]);
            let domain = String::from_utf16_lossy(&domain_buffer[..domain_size as usize]);

            if !domain.is_empty() && domain != "?" {
                Ok(format!(
                    "{}\\{}",
                    domain.trim_end_matches('\0'),
                    name.trim_end_matches('\0')
                ))
            } else {
                Ok(name.trim_end_matches('\0').to_string())
            }
        } else {
            // 如果无法解析名称，返回SID字符串
            match sid_to_string(sid) {
                Ok(sid_str) => Ok(sid_str),
                Err(_) => Ok("unresolved SID".to_string()),
            }
        }
    }
}

fn sid_to_string(sid: PSID) -> Result<String> {
    #[allow(unsafe_code)]
    unsafe {
        if IsValidSid(sid) == 0 {
            return Err(StorageError::WinAceError("Invalid SID".to_string()));
        }

        let mut sid_string_ptr: PWSTR = PWSTR(std::ptr::null_mut());
        let result =
            ConvertSidToStringSidW(sid as *mut c_void, &mut sid_string_ptr.0 as *mut *mut u16);

        if result == 0 {
            return Err(StorageError::WinAceError(
                "Failed to convert SID to string".to_string(),
            ));
        }

        let sid_string = match sid_string_ptr.to_string() {
            Ok(s) => s,
            Err(e) => {
                LocalFree(sid_string_ptr.0 as *mut _);
                return Err(StorageError::WinAceError(format!(
                    "Failed to convert SID to string: {:?}",
                    e
                )));
            }
        };
        LocalFree(sid_string_ptr.0 as *mut _);
        Ok(sid_string)
    }
}

/// 设置或取消设置文件或目录的继承保护状态
///
/// # 参数
/// * `path` - 要修改的文件或目录路径
/// * `inheritance_protect` - 如果为true，设置继承保护（禁用继承）；如果为false，取消继承保护（启用继承）
pub fn set_inheritance_protect(path: &Path, inheritance_protect: bool) -> Result<()> {
    let path_wide = path_to_wide(path);

    #[allow(unsafe_code)]
    let result = unsafe {
        // 获取当前的安全描述符
        let mut security_descriptor: *mut c_void = ptr::null_mut();
        let get_result = GetNamedSecurityInfoW(
            path_wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),          // ppsidOwner
            ptr::null_mut(),          // ppsidGroup
            ptr::null_mut(),          // ppDacl
            ptr::null_mut(),          // ppSacl
            &mut security_descriptor, // ppSecurityDescriptor
        );

        if get_result != 0 {
            return Err(StorageError::WinAceError(format!(
                "Failed to get security info, error code: {}",
                get_result
            )));
        }

        // 检查安全描述符是否有效
        let is_valid = IsValidSecurityDescriptor(security_descriptor);
        if is_valid == 0 {
            LocalFree(security_descriptor as *mut _);
            return Err(StorageError::WinAceError(format!(
                "Invalid security descriptor, error code: {}",
                GetLastError()
            )));
        }

        // 获取当前的控制标志
        let mut control: SECURITY_DESCRIPTOR_CONTROL = 0;
        let mut revision: u32 = 0;
        let get_control_success =
            GetSecurityDescriptorControl(security_descriptor, &mut control, &mut revision);

        if get_control_success == 0 {
            LocalFree(security_descriptor as *mut _);
            return Err(StorageError::WinAceError(format!(
                "Failed to get security descriptor control, error code: {}",
                GetLastError()
            )));
        }

        // 检查当前状态是否已经是所需状态
        let current_state = (control & SE_DACL_PROTECTED) != 0;
        if current_state == inheritance_protect {
            LocalFree(security_descriptor as *mut _);
            return Ok(()); // 已经是所需状态，无需操作
        }

        // 设置或清除SE_DACL_PROTECTED标志
        let set_control_success = SetSecurityDescriptorControl(
            security_descriptor,
            SE_DACL_PROTECTED, // 只修改SE_DACL_PROTECTED位
            if inheritance_protect {
                SE_DACL_PROTECTED
            } else {
                0
            }, // 根据参数设置或清除标志
        );

        if set_control_success == 0 {
            LocalFree(security_descriptor as *mut _);
            return Err(StorageError::WinAceError(format!(
                "Failed to set security descriptor control, error code: {}",
                GetLastError()
            )));
        }

        // 再次检查控制标志是否设置成功
        let mut updated_control: SECURITY_DESCRIPTOR_CONTROL = 0;
        let mut updated_revision: u32 = 0;
        let verify_success = GetSecurityDescriptorControl(
            security_descriptor,
            &mut updated_control,
            &mut updated_revision,
        );

        let expected_state = if inheritance_protect {
            SE_DACL_PROTECTED
        } else {
            0
        };
        if verify_success == 0 || (updated_control & SE_DACL_PROTECTED) != expected_state {
            LocalFree(security_descriptor as *mut _);
            return Err(StorageError::WinAceError(
                "Failed to verify security descriptor control update".to_string(),
            ));
        }

        // 应用更新后的安全描述符
        let result = SetNamedSecurityInfoW(
            path_wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(), // pSidOwner
            ptr::null_mut(), // pSidGroup
            ptr::null_mut(), // pDacl
            ptr::null_mut(), // pSacl
        );

        // 释放安全描述符
        LocalFree(security_descriptor as *mut _);

        result
    };

    if result != 0 {
        return Err(StorageError::WinAceError(format!(
            "Failed to set security info, error code: {}",
            result
        )));
    }

    Ok(())
}

/// 获取安全描述符、所有者SID、组SID和DACL
fn get_security_descriptor(
    path_wide: *const u16,
) -> Result<(*mut c_void, *mut c_void, *mut c_void, *mut ACL)> {
    let mut sd: *mut c_void = ptr::null_mut();
    let mut owner_sid: *mut c_void = ptr::null_mut();
    let mut group_sid: *mut c_void = ptr::null_mut();
    let mut dacl: *mut ACL = ptr::null_mut();

    #[allow(unsafe_code)]
    unsafe {
        let result = GetNamedSecurityInfoW(
            path_wide,
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION,
            &mut owner_sid,
            &mut group_sid,
            &mut dacl,
            ptr::null_mut(),
            &mut sd,
        );

        if result != ERROR_SUCCESS {
            return Err(StorageError::WinAceError(format!(
                "GetNamedSecurityInfoW failed: error code {}",
                result
            )));
        }
    }

    Ok((sd, owner_sid, group_sid, dacl))
}

/// 获取安全描述符的继承状态
fn get_inheritance_enabled(sd: *mut c_void) -> Result<bool> {
    let mut control: SECURITY_DESCRIPTOR_CONTROL = 0;
    let mut revision: u32 = 0;

    #[allow(unsafe_code)]
    unsafe {
        if GetSecurityDescriptorControl(sd, &mut control, &mut revision) == 0 {
            return Err(StorageError::WinAceError(
                "Failed to get security descriptor control".to_string(),
            ));
        }
    }

    // SE_DACL_PROTECTED不为0表示禁用继承
    let inheritance_enabled = (control & SE_DACL_PROTECTED) == 0;
    Ok(inheritance_enabled)
}

/// 获取非继承的ACE并创建新的ACL
fn get_explicit_aces(dacl: *mut ACL) -> Result<(*mut ACL, u32)> {
    #[allow(unsafe_code)]
    unsafe {
        let mut acl_info: ACL_SIZE_INFORMATION = std::mem::zeroed();
        let acl_info_size = std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32;

        if GetAclInformation(
            dacl,
            &mut acl_info as *mut ACL_SIZE_INFORMATION as *mut c_void,
            acl_info_size,
            ACL_REVISION as i32,
        ) == 0
        {
            return Err(StorageError::WinAceError(format!(
                "Failed to get ACL information, error code: {}",
                GetLastError()
            )));
        }

        // 创建新的ACL用于存储非继承的ACE
        let new_acl_size = std::mem::size_of::<ACL>() + (acl_info.AceCount * 128) as usize;
        let new_acl_buffer = LocalAlloc(LMEM_FIXED, new_acl_size);

        if new_acl_buffer.is_null() {
            return Err(StorageError::WinAceError(
                "Failed to allocate memory for new ACL".to_string(),
            ));
        }

        let new_acl = new_acl_buffer as *mut ACL;
        if InitializeAcl(new_acl, new_acl_size as u32, ACL_REVISION) == 0 {
            LocalFree(new_acl_buffer as *mut _);
            return Err(StorageError::WinAceError(format!(
                "Failed to initialize new ACL, error code: {}",
                GetLastError()
            )));
        }

        // 复制非继承的ACE
        let mut ace_count_copied = 0;
        for i in 0..acl_info.AceCount {
            let mut ace_ptr: *mut c_void = ptr::null_mut();
            if GetAce(dacl, i, &mut ace_ptr) == 0 {
                continue;
            }

            let ace_header = &*(ace_ptr as *const ACE_HEADER);
            let is_inherited = (ace_header.AceFlags as u32 & INHERITED_ACE) != 0;
            if is_inherited {
                continue;
            }

            let ace_size = ace_header.AceSize as u32;
            if AddAce(new_acl, ACL_REVISION, u32::MAX, ace_ptr, ace_size) == 0 {
                LocalFree(new_acl_buffer as *mut _);
                return Err(StorageError::WinAceError(format!(
                    "Failed to add ACE to new ACL, error code: {}",
                    GetLastError()
                )));
            }

            ace_count_copied += 1;
        }

        // 如果没有复制任何ACE，则释放内存并返回null
        let final_dacl = if ace_count_copied > 0 {
            new_acl
        } else {
            LocalFree(new_acl_buffer);
            ptr::null_mut()
        };

        Ok((final_dacl, ace_count_copied))
    }
}

pub fn copy_acl(source_path: &Path, target_path: &Path) -> Result<()> {
    let source_path_wide = path_to_wide(source_path);
    let target_path_wide = path_to_wide(target_path);

    // Get source DACL and inheritance information
    #[allow(unsafe_code)]
    let (source_dacl, source_inheritance_enabled, ace_count_copied) = unsafe {
        let (sd, _, _, dacl) = get_security_descriptor(source_path_wide.as_ptr())?;
        if dacl.is_null() {
            LocalFree(sd as *mut _);
            (ptr::null_mut(), false, 0)
        } else {
            // 获取非继承的ACE
            let (non_inherited_dacl, count) = get_explicit_aces(dacl)?;

            // 获取继承状态
            let inheritance_status = get_inheritance_enabled(sd)?;

            LocalFree(sd as *mut _);
            (non_inherited_dacl, inheritance_status, count)
        }
    };

    #[allow(unsafe_code)]
    unsafe {
        let (sd, _, _, _) = get_security_descriptor(target_path_wide.as_ptr())?;
        let target_inheritance_enabled = get_inheritance_enabled(sd)?;

        let needs_update =
            source_inheritance_enabled != target_inheritance_enabled || ace_count_copied > 0;
        if needs_update {
            if source_inheritance_enabled != target_inheritance_enabled {
                let set_control_success = SetSecurityDescriptorControl(
                    sd,
                    SE_DACL_PROTECTED,
                    if source_inheritance_enabled {
                        SE_DACL_PROTECTED
                    } else {
                        0
                    },
                );
                if set_control_success == 0 {
                    LocalFree(source_dacl as *mut _);
                    LocalFree(sd as *mut _);
                    return Err(StorageError::WinAceError(format!(
                        "Failed to set security descriptor control, error code: {}",
                        GetLastError()
                    )));
                }
            }

            // Apply the new DACL and updated security descriptor
            let set_result = SetNamedSecurityInfoW(
                target_path_wide.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                source_dacl,
                ptr::null_mut(),
            );

            if set_result != ERROR_SUCCESS {
                LocalFree(source_dacl as *mut _);
                LocalFree(sd as *mut _);
                return Err(StorageError::WinAceError(format!(
                    "Failed to set security info for target, error code: {}",
                    set_result
                )));
            }
            LocalFree(sd as *mut _);
        }

        // Clean up resources
        if ace_count_copied > 0 {
            LocalFree(source_dacl as *mut _);
        }
    }

    Ok(())
}

/// 读取文件的完整安全描述符为字节（用于跨进程 ACL 传输）
pub fn get_acl_bytes(path: &Path) -> Result<Vec<u8>> {
    let path_wide = path_to_wide(path);

    #[allow(unsafe_code)]
    unsafe {
        let (sd, _, _, _) = get_security_descriptor(path_wide.as_ptr())?;
        if sd.is_null() {
            return Ok(Vec::new());
        }

        let len = GetSecurityDescriptorLength(sd) as usize;
        if len == 0 {
            LocalFree(sd as *mut _);
            return Ok(Vec::new());
        }

        let bytes = std::slice::from_raw_parts(sd as *const u8, len).to_vec();
        LocalFree(sd as *mut _);
        Ok(bytes)
    }
}

/// 从字节设置文件的安全描述符（用于跨进程 ACL 传输）
pub fn set_acl_bytes(path: &Path, acl_data: &[u8]) -> Result<()> {
    if acl_data.is_empty() {
        return Ok(());
    }

    let path_wide = path_to_wide(path);

    #[allow(unsafe_code)]
    unsafe {
        let sd_ptr = acl_data.as_ptr() as *mut c_void;

        if IsValidSecurityDescriptor(sd_ptr) == 0 {
            return Err(StorageError::WinAceError(
                "Invalid security descriptor bytes".to_string(),
            ));
        }

        let mut dacl_present: i32 = 0;
        let mut dacl: *mut ACL = ptr::null_mut();
        let mut dacl_defaulted: i32 = 0;
        GetSecurityDescriptorDacl(sd_ptr, &mut dacl_present, &mut dacl, &mut dacl_defaulted);

        let dacl_to_set = if dacl_present != 0 && !dacl.is_null() {
            dacl
        } else {
            ptr::null_mut()
        };

        let result = SetNamedSecurityInfoW(
            path_wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            dacl_to_set,
            ptr::null_mut(),
        );

        if result != ERROR_SUCCESS {
            return Err(StorageError::WinAceError(format!(
                "SetNamedSecurityInfoW failed: error code {}",
                result
            )));
        }
    }

    Ok(())
}
