FROM nginx:alpine

COPY output/best_nodes.txt /usr/share/nginx/html/sub
COPY output/report.json /usr/share/nginx/html/report
