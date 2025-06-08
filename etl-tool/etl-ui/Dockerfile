# Stage 1: Build
FROM node:18-alpine AS builder
WORKDIR /app

# Copy package.json and package-lock.json (or npm-shrinkwrap.json)
COPY package*.json ./

# Install dependencies
# Using npm ci for cleaner installs based on package-lock.json
RUN npm ci

# Copy the rest of the application code
COPY . .

# Build the Next.js application
RUN npm run build

# Stage 2: Runtime
FROM node:18-alpine
WORKDIR /app

ENV NODE_ENV production
ARG UI_PORT=3000
ENV PORT=${UI_PORT} # Next.js uses PORT environment variable

# Copy built assets from the builder stage
COPY --from=builder /app/public ./public
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
# package.json is needed to run `npm start` which typically calls `next start`
COPY --from=builder /app/package.json ./package.json
# next.config.js might be needed by `next start`
COPY --from=builder /app/next.config.js ./next.config.js

EXPOSE ${UI_PORT}

# The default start command for Next.js is `next start`
# which is usually aliased to `npm start` in package.json
CMD ["npm", "start"]
