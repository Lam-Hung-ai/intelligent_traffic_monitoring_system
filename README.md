# intelligent_traffic_monitoring_system
## 1. Thành viên 
- Nguyễn Văn Lâm Hùng
- Phan Anh
- Lê Sỹ Long Nhật
- Vũ Trí Trường - 2351260692

## 2. Hướng dẫn làm việc
- Khuyến nghị mọi người cài đặt Ruff extention trên VSCode để điều chỉnh định dạng chuẩn (chi cần lưu là Ruff extention sẽ định dạng lại file chuẩn quốc tế)
- Nếu chưa tải repo về máy thì chạy:
```cmd
git clone https://github.com/Lam-Hung-ai/intelligent_traffic_monitoring_system.git
```
- Khi muốn thay đổi code thì tạo nhánh mới:
```cmd
git branch ten_cua_ban  # Nhánh mới mang tên bạn
git branch -a # Xem tất cả các nhánh
git checkout ten_cua_ban # Chuyển sang nhánh mới để làm việc
```
- Khi muốn push code lên repo chung thì:
```cmd
git status      #Xem trạng thái
git add .       # Thêm các file sửa đổi vào git local
git status      #Xem trạng thái
git commit -m "update"      #Xác nhận thay đổi code
git push -u oringin ten_cua_ban     # Đẩy code lên repo với nhánh ten_cua_ban. # Chờ mình xác nhận merge code lên nhánh main
```
- Khi muốn cập nhật code đồng bộ với repo github:
```cmd
git pull --no-rebase
```