"use client";

import { useEffect, useRef } from "react";

interface Point {
  x: number;
  y: number;
  z: number;
  lat: number;
  lng: number;
  visible: boolean;
  brightness: number;
}

interface Connection {
  from: Point;
  to: Point;
  color: string;
  opacity: number;
}

const AnimatedGlobe = () => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animationRef = useRef<number | null>(null);
  const rotationRef = useRef(0);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const centerX = canvas.width / 2;
    const centerY = canvas.height / 2;
    const radius = Math.min(centerX, centerY) - 60;

    // Generate globe points with realistic Earth distribution
    const points: Point[] = [];
    const gridSize = 4;

    for (let lat = -90; lat <= 90; lat += gridSize) {
      for (let lng = -180; lng <= 180; lng += gridSize) {
        const latRad = (lat * Math.PI) / 180;
        const lngRad = (lng * Math.PI) / 180;

        // Create more realistic distribution - denser around continents
        let density = 0.6;
        
        // North America
        if (lat > 20 && lat < 70 && lng > -140 && lng < -50) density = 0.9;
        // Europe/Asia
        if (lat > 35 && lat < 70 && lng > -10 && lng < 140) density = 0.85;
        // Africa
        if (lat > -35 && lat < 37 && lng > -20 && lng < 50) density = 0.8;
        // South America
        if (lat > -55 && lat < 12 && lng > -85 && lng < -35) density = 0.8;
        // Australia
        if (lat > -45 && lat < -10 && lng > 110 && lng < 155) density = 0.7;
        
        if (Math.random() > density) continue;

        const x = radius * Math.cos(latRad) * Math.cos(lngRad);
        const y = radius * Math.sin(latRad);
        const z = radius * Math.cos(latRad) * Math.sin(lngRad);

        points.push({
          x,
          y,
          z,
          lat,
          lng,
          visible: true,
          brightness: 0.8 + Math.random() * 0.4,
        });
      }
    }

    // Create connections
    const connections: Connection[] = [];
    const connectionColors = ["#22d3ee", "#3b82f6", "#10b981"];

    const majorNodes = points.filter((p, i) => i % 30 === 0).slice(0, 20);
    for (let i = 0; i < majorNodes.length; i++) {
      for (let j = i + 1; j < Math.min(i + 4, majorNodes.length); j++) {
        if (Math.random() > 0.7) {
          connections.push({
            from: majorNodes[i],
            to: majorNodes[j],
            color: connectionColors[Math.floor(Math.random() * connectionColors.length)],
            opacity: 0.6 + Math.random() * 0.4,
          });
        }
      }
    }

    for (let i = 0; i < 15; i++) {
      const from = points[Math.floor(Math.random() * points.length)];
      const to = points[Math.floor(Math.random() * points.length)];
      connections.push({
        from,
        to,
        color: connectionColors[Math.floor(Math.random() * connectionColors.length)],
        opacity: 0.4 + Math.random() * 0.3,
      });
    }

    const animate = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      rotationRef.current += 0.005;

      // Transform points with rotation and 45-degree tilt
      const rotatedPoints = points.map((point) => {
        const x = point.x * Math.cos(rotationRef.current) - point.z * Math.sin(rotationRef.current);
        const y = point.y;
        const z = point.x * Math.sin(rotationRef.current) + point.z * Math.cos(rotationRef.current);

        const tiltAngle = Math.PI / 4;
        const newY = y * Math.cos(tiltAngle) - z * Math.sin(tiltAngle);
        const newZ = y * Math.sin(tiltAngle) + z * Math.cos(tiltAngle);

        return {
          ...point,
          x: x + centerX,
          y: newY + centerY,
          z: newZ,
          visible: newZ > 0,
        };
      });

      // Draw connections
      connections.forEach((connection) => {
        const fromPoint = rotatedPoints.find(p => 
          p.lat === connection.from.lat && p.lng === connection.from.lng
        );
        const toPoint = rotatedPoints.find(p => 
          p.lat === connection.to.lat && p.lng === connection.to.lng
        );

        if (fromPoint?.visible && toPoint?.visible) {
          const gradient = ctx.createLinearGradient(
            fromPoint.x, fromPoint.y,
            toPoint.x, toPoint.y
          );
          gradient.addColorStop(0, `${connection.color}${Math.floor(connection.opacity * 255).toString(16)}`);
          gradient.addColorStop(0.5, `${connection.color}FF`);
          gradient.addColorStop(1, `${connection.color}${Math.floor(connection.opacity * 255).toString(16)}`);

          ctx.strokeStyle = gradient;
          ctx.lineWidth = 2;
          ctx.beginPath();
          ctx.moveTo(fromPoint.x, fromPoint.y);
          
          const midX = (fromPoint.x + toPoint.x) / 2;
          const midY = (fromPoint.y + toPoint.y) / 2 - 30;
          ctx.quadraticCurveTo(midX, midY, toPoint.x, toPoint.y);
          ctx.stroke();
        }
      });

      // Draw globe points
      rotatedPoints.forEach((point) => {
        if (point.visible) {
          const distance = Math.sqrt(point.z * point.z + radius * radius);
          const scale = Math.max(0.3, 1 - distance / (radius * 2));
          const size = 1.5 * scale * point.brightness;
          
          const pointGradient = ctx.createRadialGradient(
            point.x, point.y, 0,
            point.x, point.y, size * 3
          );
          pointGradient.addColorStop(0, `rgba(255, 255, 255, ${0.8 * scale})`);
          pointGradient.addColorStop(0.5, `rgba(34, 211, 238, ${0.4 * scale})`);
          pointGradient.addColorStop(1, "rgba(34, 211, 238, 0)");

          ctx.fillStyle = pointGradient;
          ctx.beginPath();
          ctx.arc(point.x, point.y, size * 3, 0, Math.PI * 2);
          ctx.fill();

          ctx.fillStyle = `rgba(255, 255, 255, ${scale * point.brightness})`;
          ctx.beginPath();
          ctx.arc(point.x, point.y, size, 0, Math.PI * 2);
          ctx.fill();
        }
      });

      animationRef.current = requestAnimationFrame(animate);
    };

    animate();

    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, []);

  return (
    <div className="relative w-full h-full flex items-center justify-center">
      <canvas
        ref={canvasRef}
        width={600}
        height={600}
        className="max-w-full max-h-full rounded-xl"
        style={{
          filter: "drop-shadow(0 0 20px rgba(34, 211, 238, 0.3))",
        }}
      />
    </div>
  );
};

export default AnimatedGlobe;
