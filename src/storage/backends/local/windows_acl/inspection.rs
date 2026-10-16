use std::ffi::c_void;
use std::io::Result;
use std::path::{Path, PathBuf};
use std::{fmt, io};
use windows::core::PWSTR;
use windows_sys::Win32::Foundation::{LocalFree, MAX_PATH};
use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACCESS_DENIED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION,
    AclSizeInformation, GetAce, GetAclInformation, INHERITED_ACE, IsValidSid, LookupAccountSidW,
    PSID, SID, SID_NAME_USE,
};
use windows_sys::Win32::System::SystemServices::{ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE};

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

/// Inspects a file or directory's native Windows discretionary ACL.
pub fn get_security_info(path: &Path) -> Result<SecurityInfo> {
    let file = super::open_path(path, false)?;
    let descriptor = super::Descriptor::read(&file)?;
    let (owner_sid, group_sid, dacl) = descriptor.components();
    let owner = if owner_sid.is_null() {
        "no owner".to_string()
    } else {
        lookup_sid_name(owner_sid)?
    };
    let primary_group = if group_sid.is_null() {
        "no group".to_string()
    } else {
        lookup_sid_name(group_sid)?
    };
    Ok(SecurityInfo {
        path: path.to_path_buf(),
        owner,
        primary_group,
        has_dacl: !dacl.is_null(),
        aces: if dacl.is_null() {
            Vec::new()
        } else {
            parse_dacl_entries(dacl)?
        },
        security_descriptor: descriptor.bytes(),
        inheritance_enabled: !descriptor.protected()?,
    })
}

fn parse_dacl_entries(dacl: *mut ACL) -> Result<Vec<AceInfo>> {
    let mut aces = Vec::new();

    #[expect(unsafe_code, reason = "Win32 ACL APIs require audited raw-pointer FFI")]
    unsafe {
        let mut acl_size_info: ACL_SIZE_INFORMATION = std::mem::zeroed();

        if GetAclInformation(
            dacl,
            &mut acl_size_info as *mut _ as *mut _,
            std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        ) == 0
        {
            return Err(io::Error::other(
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
    #[expect(unsafe_code, reason = "Win32 ACL APIs require audited raw-pointer FFI")]
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
            _ => {
                return Some(AceInfo {
                    trustee: "unparsed trustee".to_string(),
                    trustee_type: "unknown".to_string(),
                    access_mode: "other".to_string(),
                    inherited: ace_flags & INHERITED_ACE as u8 != 0,
                    mask: 0,
                    ace_flags,
                    ace_type,
                });
            }
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

    #[expect(unsafe_code, reason = "Win32 ACL APIs require audited raw-pointer FFI")]
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
    #[expect(unsafe_code, reason = "Win32 ACL APIs require audited raw-pointer FFI")]
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
    #[expect(unsafe_code, reason = "Win32 ACL APIs require audited raw-pointer FFI")]
    unsafe {
        if IsValidSid(sid) == 0 {
            return Err(io::Error::other("Invalid SID".to_string()));
        }

        let mut sid_string_ptr: PWSTR = PWSTR(std::ptr::null_mut());
        let result =
            ConvertSidToStringSidW(sid as *mut c_void, &mut sid_string_ptr.0 as *mut *mut u16);

        if result == 0 {
            return Err(io::Error::other(
                "Failed to convert SID to string".to_string(),
            ));
        }

        let sid_string = match sid_string_ptr.to_string() {
            Ok(s) => s,
            Err(e) => {
                LocalFree(sid_string_ptr.0 as *mut _);
                return Err(io::Error::other(format!(
                    "Failed to convert SID to string: {:?}",
                    e
                )));
            }
        };
        LocalFree(sid_string_ptr.0 as *mut _);
        Ok(sid_string)
    }
}
