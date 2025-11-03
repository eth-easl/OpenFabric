import { Pool } from 'pg';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { nanoid } from 'nanoid';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Initialize database tables
export async function initializeDatabase() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        email VARCHAR(255) UNIQUE NOT NULL,
        stytch_id VARCHAR(255) UNIQUE NOT NULL,
        wallet_address VARCHAR(255),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS api_keys (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        key_hash VARCHAR(255) NOT NULL,
        name VARCHAR(255) NOT NULL,
        prefix VARCHAR(50) NOT NULL,
        permissions TEXT[] DEFAULT ARRAY['read'],
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        last_used TIMESTAMP WITH TIME ZONE,
        expires_at TIMESTAMP WITH TIME ZONE,
        is_active BOOLEAN DEFAULT true
      );
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);
      CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
      CREATE INDEX IF NOT EXISTS idx_users_stytch_id ON users(stytch_id);
    `);

    console.log('Database tables initialized successfully');
  } catch (error) {
    console.error('Error initializing database:', error);
    throw error;
  }
}

// User operations
export async function createOrUpdateUser(userData: {
  email: string;
  stytch_id: string;
  wallet_address?: string;
}) {
  try {
    const result = await pool.query(
      `INSERT INTO users (email, stytch_id, wallet_address)
       VALUES ($1, $2, $3)
       ON CONFLICT (stytch_id)
       DO UPDATE SET
         email = EXCLUDED.email,
         wallet_address = COALESCE(EXCLUDED.wallet_address, users.wallet_address),
         updated_at = CURRENT_TIMESTAMP
       RETURNING *`,
      [userData.email, userData.stytch_id, userData.wallet_address]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error creating/updating user:', error);
    throw error;
  }
}

export async function getUserByStytchId(stytch_id: string) {
  try {
    const result = await pool.query('SELECT * FROM users WHERE stytch_id = $1', [stytch_id]);
    return result.rows[0] || null;
  } catch (error) {
    console.error('Error getting user by Stytch ID:', error);
    throw error;
  }
}

// API Key operations
export async function generateAPIKey(userId: string, name: string, permissions: string[] = ['read']) {
  try {
    const prefix = process.env.API_KEY_PREFIX || 'sk-rc-';
    const randomPart = nanoid(32);
    const apiKey = `${prefix}${randomPart}`;
    const keyHash = await bcrypt.hash(apiKey, 12);

    const result = await pool.query(
      `INSERT INTO api_keys (user_id, key_hash, name, prefix, permissions)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [userId, keyHash, name, prefix, permissions]
    );

    return {
      ...result.rows[0],
      plain_key: apiKey // Only return this once during creation
    };
  } catch (error) {
    console.error('Error generating API key:', error);
    throw error;
  }
}

export async function validateAPIKey(apiKey: string) {
  try {
    const prefix = process.env.API_KEY_PREFIX || 'sk-rc-';
    if (!apiKey.startsWith(prefix)) {
      return null;
    }

    // Get all API keys with the same prefix
    const result = await pool.query(
      `SELECT ak.*, u.email, u.wallet_address
       FROM api_keys ak
       JOIN users u ON ak.user_id = u.id
       WHERE ak.prefix = $1 AND ak.is_active = true`,
      [prefix]
    );

    for (const keyData of result.rows) {
      const isValid = await bcrypt.compare(apiKey, keyData.key_hash);
      if (isValid) {
        // Update last used timestamp
        await pool.query(
          'UPDATE api_keys SET last_used = CURRENT_TIMESTAMP WHERE id = $1',
          [keyData.id]
        );
        return keyData;
      }
    }

    return null;
  } catch (error) {
    console.error('Error validating API key:', error);
    throw error;
  }
}

export async function getUserAPIKeys(userId: string) {
  try {
    const result = await pool.query(
      'SELECT id, name, permissions, created_at, last_used, expires_at, is_active FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC',
      [userId]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting user API keys:', error);
    throw error;
  }
}

export async function revokeAPIKey(keyId: string, userId: string) {
  try {
    const result = await pool.query(
      'UPDATE api_keys SET is_active = false WHERE id = $1 AND user_id = $2 RETURNING *',
      [keyId, userId]
    );
    return result.rows[0] || null;
  } catch (error) {
    console.error('Error revoking API key:', error);
    throw error;
  }
}

// JWT operations
export function generateJWT(user: any) {
  if (!process.env.JWT_SECRET) {
    throw new Error('JWT_SECRET is not defined');
  }

  return jwt.sign(
    {
      userId: user.id,
      email: user.email,
      stytchId: user.stytch_id,
      walletAddress: user.wallet_address
    },
    process.env.JWT_SECRET,
    { expiresIn: '7d' }
  );
}

export function verifyJWT(token: string) {
  if (!process.env.JWT_SECRET) {
    throw new Error('JWT_SECRET is not defined');
  }

  try {
    return jwt.verify(token, process.env.JWT_SECRET);
  } catch (error) {
    return null;
  }
}

export { pool };